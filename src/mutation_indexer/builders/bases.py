import abc
import copy
import dataclasses
import functools
import itertools
import logging
import operator
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence, Set
from importlib import resources
from typing import (
    Literal,
    Protocol,
    TypeGuard,
    get_type_hints,
    runtime_checkable,
)

import more_itertools
from gdcmodels import esmodels
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import es_utils, pyspark_extensions, schemas
from mutation_indexer.configuration import builders
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite

logger = logging.getLogger(__name__)

BASE_PRIMARY_ALIQUOT_FIELDS = frozenset(
    (
        "file_id",
        "created_datetime",
        "cases.case_id",
        "cases.samples.sample_id",
        "cases.samples.sample_type",
    )
)


@runtime_checkable
class Builder(Protocol):
    """A class which can build its defined output dataframe from its required inputs."""

    def __hash__(self) -> int:
        return hash(self.output)

    def __eq__(self, value: object) -> bool:
        if value is self:
            return True

        if isinstance(value, Builder):
            return self.output == value.output

        if isinstance(value, build.DataFrame):
            return self.output == value

        return False

    @property
    def output(self) -> build.DataFrame:  # type: ignore
        """The data frame which will be produced by this builder."""
        pass

    @property
    def inputs(self) -> Iterable[build.DataFrame]:  # type: ignore
        """The required data frames to build this builder's output."""
        pass

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:  # type: ignore
        """
        From the given inputs builds the defined output data frame.

        Args:
            inputs: A set of input data frames that MUST include the data frames
                defined in the implementing class's inputs property.

        Returns:
            A data frame which contains the expected data of the defined output
            property.
        """
        pass


class InputDataFrameManger[TInputDFs: Mapping[str, object]]:
    __slots__ = ("_required_dfs", "_required_params")

    def __init__(self, input_type: type[TInputDFs]) -> None:
        type_hints = get_type_hints(input_type)

        assert all(issubclass(t, sql.DataFrame) for t in type_hints.values()), (
            "Input mapping type must contain only sql.DataFrames"
        )

        self._required_params: Set[str] = type_hints.keys()
        self._required_dfs = tuple(
            build.DataFrame.from_param(p) for p in self._required_params
        )

    @property
    def required_dataframes(self) -> Iterable[build.DataFrame]:
        """
        All required data frames needed as inputs for the given input's TypedDict.
        """
        return self._required_dfs

    def check(self, inputs: Mapping[str, sql.DataFrame]) -> TypeGuard[TInputDFs]:
        """
        Checks if all required keys for the input's TypedDict are present in the input
        mapping.

        Returns:
            True if the inputs mapping is an instance of the desired input_dfs.
        """
        return self._required_params <= inputs.keys()


class InputBuilder[TConfig: builders.Builder, TInputDFs: Mapping[str, object]](
    Builder, abc.ABC
):
    __slots__ = ("_config", "_input_manager", "_output", "_spark_session")

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        input_type: type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        self._config = config
        self._spark_session = spark_session
        self._input_manager = InputDataFrameManger(input_type)
        self._output = output

    @property
    def output(self) -> build.DataFrame:
        return self._output

    @property
    def inputs(self) -> Iterable[build.DataFrame]:
        return self._input_manager.required_dataframes

    @abc.abstractmethod
    def _build_from_scratch(self, input_dfs: TInputDFs) -> sql.DataFrame:
        """
        The functionality to build a new data frame from the required inputs.

        Args:
            input_dfs: The required data frames to construct the output data frame.

        Returns:
            A data frame which contains the expected data of the defined output.
        """
        pass

    def _safe_read(self) -> sql.DataFrame:
        """
        Safely reads a backed up parquet file into a data frame. If the file does not
        exist then an error will be raised.

        Returns:
            A data frame with data from the file at the configured backup path
        """
        logger.info(f"Reading: {self.output.name}")

        return self._spark_session.read.parquet(self._config.backup.path)

    def _read(self) -> sql.DataFrame | None:
        """
        Reads the data frame, if configured to READ, from the configure parquet file. If
        the builder is not configured to read then None is returned.

        Returns:
            An optional data frame based on the configured backup mode.
        """
        if self._config.backup.mode == build.BackupMode.READ:
            return self._safe_read()

        return None

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Writes the data frame to any configured or required data store. I.e. memory,
        disk, or elasticsearch. If the df is written to disk and the backup mode is
        BOTH than the written dataframe will be returned.

        Returns:
            The original, cached, or written data frame depending on the builders
            configuration.
        """
        if self._config.backup.mode.is_write():
            logger.info(f"Writing: {self.output.name}")
            self._write_backup(df)

        if self._config.backup.mode == build.BackupMode.BOTH:
            return self._safe_read()

        return df.cache() if self._config.is_cached else df

    def _write_backup(self, df: sql.DataFrame) -> None:
        df.write.parquet(self._config.backup.path, mode="overwrite")

    def build(self, **inputs: sql.DataFrame) -> sql.DataFrame:
        assert self._input_manager.check(inputs), "Missing required inputs."

        df = self._read()

        if not df:
            logger.info(f"Building: {self.output.name}")

            df = self._build_from_scratch(inputs)

        return self._write(df)


def _combine_weighted_entity_dfs(
    weighted_file_df: sql.DataFrame | None,
    weighted_case_df: sql.DataFrame | None,
) -> sql.DataFrame:
    if weighted_case_df and weighted_file_df:
        return weighted_case_df.union(weighted_file_df)

    elif weighted_case_df:
        return weighted_case_df

    elif weighted_file_df:
        return weighted_file_df

    else:
        raise ValueError("At least one valid entity must be provided.")


def _add_required_include_fields(
    include_fields: Iterable[str] | Literal[True],
) -> Collection[str]:
    if include_fields is not True:
        return BASE_PRIMARY_ALIQUOT_FIELDS.union(include_fields)

    return BASE_PRIMARY_ALIQUOT_FIELDS


class PrimaryAliquotBuilder[TConfig: builders.Builder, TInputDFs: Mapping[str, object]](
    InputBuilder[TConfig, TInputDFs]
):
    __slots__ = ("_additional_selections", "_es_dataframe_util")

    @dataclasses.dataclass(frozen=True)
    class Weight:
        condition: sql.Column
        value: int

        def __add__(
            self, other: "PrimaryAliquotBuilder.Weight"
        ) -> "PrimaryAliquotBuilder.Weight":
            return PrimaryAliquotBuilder.Weight(
                self.condition & other.condition, self.value + other.value
            )

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        input_type: type[TInputDFs],
        output: build.DataFrame,
        additional_selections: Iterable[str] = (),
    ) -> None:
        """
        Args:
            config: The configuration for the given builder.
            sql_context: The sql session object for the current pyspark run.
            es_dataframe_util: The util for creating dataframes from data in
                elasticsearch.
            output: The DataFrame which is the resulting output of this builder.
            additional_selections: An additional set of fields to include when selecting
                data from the newly created primary aliquot data frame.

        """
        super().__init__(config, spark_session, input_type, output)

        self._es_dataframe_util = es_dataframe_util
        self._additional_selections = additional_selections

    def _get_weighted_entity_df(
        self,
        weighted_df: sql.DataFrame,
        entity_id: str,
        entity: str,
    ) -> sql.DataFrame:
        return weighted_df.select(
            F.col(entity_id).alias("entity_id"),
            F.lit(entity).alias("entity"),
            "file_id",
            "created_datetime",
            "case_id",
            "sample_id",
            "case",
            "submitter_id",
            "_weight",
            *self._additional_selections,
        )

    def _get_initial_weighted_df(
        self,
        query: dict,
        include_fields: Collection[str] | Literal[True],
    ) -> sql.DataFrame:
        """
        Gets the initial data from elasticsearch. This is the data meeting the
        criteria in the query and includes the fields given in include_fields.

        NOTE: Override this method if any manipulation of the data frame needs to
        happen before the standard primary aliquot selection begins. E.g. use it to
        alias fields that have special characters that cannot be utilized in
        additional_selections

        Args:
            query: The query to be run in elasticsearch to determine the data loaded.
            include_fields: The fields that will be included/returned in the dataframe.
                If set to True, all fields are returned.

        Returns:
            The data frame created in the above process.
        """
        return self._es_dataframe_util.read(
            build.IndexType.FILE,
            source_filter=include_fields,
            query=query,
        )

    def _weight_matrix(self) -> Sequence[Sequence[sql.Column]]:
        """A matrix of conditions used to weight the aliquots for selection.

        The matrix is represented as an ordered collection of sequences where each
        sequence is a dimension in the matrix. The most desireable conditions in each
        dimension should be listed first, but dimensions should be listed least to most
        important.

        NOTE: see _convert_weight_matrix method for more details.
        NOTE: This needs to be calculated at runtime AFTER the spark session has been
            initiated otherwise F.col/F.lit will fail to be instantiated.
        """
        sample_type = F.col("sample_type")

        return (
            (
                sample_type == F.lit("Primary Tumor"),  # <-- highest priority
                sample_type == F.lit("Primary Blood Derived Cancer - Bone Marrow"),
                sample_type == F.lit("Primary Blood Derived Cancer - Peripheral Blood"),
                sample_type == F.lit("Metastatic"),
                sample_type == F.lit("Additional Metastatic"),
                sample_type == F.lit("Recurrent Tumor"),
                sample_type == F.lit("Recurrent Blood Derived Cancer - Bone Marrow"),
                sample_type == F.lit("Recurrent Blood Derived Cancer - Peripheral Blood"),
                sample_type == F.lit("Additional - New Primary"),
                F.lit(1) == F.lit(1),  # This is a default value.
            ),
        )

    def _convert_weight_matrix(self) -> Iterable[Weight]:
        """Get the wights to be associated with each sample row.

        This translates the base matrix to a single set of weighted values to apply to
        the sample row. It does this by first calculating the magnitude of the matrix
        which is equal to the length of the largest dimension. Then weights are
        calculated based on the order in the dimension (the dimension weight) times the
        order of magnitude where the order of magnitude is determined by the order of
        the dimension in the weight matrix. The matrix is then flattened by combining
        all combinations of weight conditions and adding all of their unique values to
        calculate their total weight.

        condition: (dimension weight) * (magnitude ** order) = (weight)
        condition0 & condition1: (weight0) + (weight1) = (total weight)

        Example:
            weight_matrix (implied weight within dimension):
                <0 order>
                    sample_type == "Tumor": (0)
                    sample_type == "Metastatic": (1)
                    sample_type == "Normal": (2)
                <1st order>
                    workflow_type == "ABSOLUTE": (0)
                    workflow_type == "ASCAT": (1)

            weights:
                sample_type == "Tumor & workflow_type == "ABSOLUTE":
                    (0 * (3 ** 0)) + (0 * (3 ** 1)) = 0
                sample_type == "Metastatic" & workflow_type == "ABSOLUTE":
                    (1 * (3 ** 0)) + (0 * (3 ** 1)) = 1
                sample_type == "Normal" & workflow_type == "ABSOLUTE":
                    (2 * (3 ** 0)) + (0 * (3 ** 1)) = 2
                sample_type == "Tumor & workflow_type == "ASCAT":
                    (0 * (3 ** 0)) + (1 * (3 ** 1)) = 3
                sample_type == "Metastatic" & workflow_type == "ASCAT":
                    (1 * (3 ** 0)) + (1 * (3 ** 1)) = 4
                sample_type == "Normal" & workflow_type == "ASCAT":
                    (2 * (3 ** 0)) + (1 * (3 ** 1)) = 5

        Returns:
            The weights to be applied to the sample rows in the weighted data frame.
        """
        weight_matrix = self._weight_matrix()

        def _convert_to_weights() -> Iterator[Iterator[PrimaryAliquotBuilder.Weight]]:
            magnitude = max(len(d) for d in weight_matrix)

            for order, dimension in enumerate(weight_matrix):
                yield (
                    PrimaryAliquotBuilder.Weight(condition, weight * (magnitude**order))
                    for weight, condition in enumerate(dimension)
                )

        return (
            functools.reduce(operator.add, weights)
            for weights in itertools.product(*_convert_to_weights())
        )

    def _weight_col(self) -> sql.Column:
        """
        Builds the sample weight column based on the weights in the weight matrix.

        Returns:
            The `_weight` column.
        """
        when_clause = F.when(F.lit(1) != F.lit(1), 0)  # dummy when clause

        return functools.reduce(
            lambda c, w: c.when(w.condition, w.value),
            self._convert_weight_matrix(),
            when_clause,
        )

    def _get_weighted_df(
        self,
        query: dict,
        include_fields: Collection[str] | Literal[True],
    ) -> sql.DataFrame:
        return (
            self._get_initial_weighted_df(query, include_fields)
            .select(
                "file_id",
                F.col("created_datetime").cast("timestamp"),
                pyspark_extensions.explode_nested_doc("cases").alias("case"),
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                F.col("case.case_id").alias("case_id"),
                "case",
                F.explode("case.samples").alias("sample"),
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "case",
                "sample.sample_id",
                "sample.sample_type",
                "sample.submitter_id",
                *self._additional_selections,
            )
            .select(
                "file_id",
                "created_datetime",
                "case_id",
                "sample_id",
                "submitter_id",
                "case",
                self._weight_col().alias("_weight"),
                *self._additional_selections,
            )
        )

    def _get_primary_aliquot_df(
        self,
        filters: Iterable[dict],
        entities: Set[Literal["case", "file"]] = frozenset(("case", "file")),
        include_fields: Iterable[str] | Literal[True] = True,
    ) -> sql.DataFrame:
        """
        Args:
            filters: The filters used to query es files with
            include_fields: An optional field used to tell spark which fields to read from
                spark. Use to include extra fields in the returned case mapping.

        Returns:
            a dataframe with the file data associated with the most relevant sample for
            each case.

            file_id
            created_datetime
            experimental_strategy
            case_id
            case
                case_id
                samples
                (other case fields can be included in the include fields param)
        """
        query = {"query": {"bool": {"must": filters}}}
        include_fields = _add_required_include_fields(include_fields)
        weighted_df = self._get_weighted_df(query, include_fields)
        weighted_file_df = None
        weighted_case_df = None

        if "file" in entities:
            weighted_file_df = self._get_weighted_entity_df(weighted_df, "file_id", "file")

        if "case" in entities:
            weighted_case_df = self._get_weighted_entity_df(weighted_df, "case_id", "case")

        weighted_entity_df = _combine_weighted_entity_dfs(weighted_file_df, weighted_case_df)
        entity_window = (
            sql.Window()
            .partitionBy("entity", "entity_id")
            .orderBy(
                F.col("_weight"),
                F.col("created_datetime"),
                F.col("file_id"),
            )
        )

        return (
            weighted_entity_df.withColumn("row_number", F.row_number().over(entity_window))
            .where(F.col("row_number") == 1)
            .select(
                "entity_id",
                "entity",
                "file_id",
                "created_datetime",
                "case_id",
                "sample_id",
                "case",
                "submitter_id",
                *self._additional_selections,
            )
        )


def _expand_aliquots(aliquot_df: sql.DataFrame) -> sql.DataFrame:
    """
    Expands the rows in the loaded file data to be each a single aliquot worth of data.

    Args:
        aliquot_df: The data frame containing the file data containing all associated
            aliquots under the cases field.

    Returns:
        A dataframe fo the aliquot data.
    """
    return (
        aliquot_df.select("file_id", F.explode_outer("cases").alias("case"))
        .select(
            "file_id",
            F.col("case.case_id").alias("case_id"),
            F.explode_outer("case.samples").alias("sample"),
        )
        .select(
            "file_id",
            "case_id",
            F.col("sample.sample_id").alias("sample_id"),
            F.explode_outer("sample.portions").alias("portion"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("portion.analytes").alias("analyte"),
        )
        .select(
            "file_id",
            "case_id",
            "sample_id",
            F.explode_outer("analyte.aliquots").alias("aliquot"),
        )
    )


class InclusivePrimaryAliquotBuilder[
    TConfig: builders.Builder,
    TInputDFs: Mapping[str, object],
](PrimaryAliquotBuilder[TConfig, TInputDFs]):
    """
    This builder creates a primary aliquot dataframe which INCLUDES the aliquot data
    associated with the sample which has been identified as the "primary" aliquot.
    """

    __slots__ = ("_es_rdd_util",)

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
        input_type: type[TInputDFs],
        output: build.DataFrame,
        additional_selections: Iterable[str] = (),
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            input_type,
            output,
            additional_selections,
        )

        self._es_rdd_util = es_rdd_util

    def _get_aliquot_level_df(self, filters: Iterable[dict]) -> sql.DataFrame:
        """
        Loads the aliquot data from elasticsearch.

        Args:
            filters: The filters with which to restrict the aliquots loaded.

        Returns:
            A data frame containing the desired aliquot data

            aliquot {}
            |---file_id
            |---case_id
            |---sample_id
            |---aliquot_id
            +---aliquot_created_datetime
        """
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "nested": {
                                "path": "cases.samples.portions.analytes.aliquots",
                                "query": {
                                    "exists": {
                                        "field": "cases.samples.portions.analytes.aliquots"
                                    }
                                },
                            }
                        },
                        *filters,
                    ]
                }
            }
        }
        included_fields = (
            "file_id",
            "cases.case_id",
            "cases.samples.sample_id",
            "cases.samples.portions.analytes.aliquots.aliquot_id",
            "cases.samples.portions.analytes.aliquots.created_datetime",
        )
        aliquot_data_schema = schemas.load_schema("builders/primary_aliquot/aliquot_data.json")

        if self._config.projects:
            project_clause = {
                "nested": {
                    "path": "cases",
                    "query": {"terms": {"cases.project.project_id": self._config.projects}},
                }
            }

            query["query"]["bool"]["must"].append(project_clause)

        aliquot_df = _expand_aliquots(
            self._es_rdd_util.get_rdd(
                build.IndexType.FILE, include_fields=included_fields, query=query
            )
            .toDF(aliquot_data_schema)
            .select("_source.*")
        ).select(
            "file_id",
            "case_id",
            "sample_id",
            "aliquot.aliquot_id",
            F.col("aliquot.created_datetime")
            .cast("timestamp")
            .alias("aliquot_created_datetime"),
        )

        return aliquot_df

    def _get_primary_aliquot_df(
        self,
        filters: Iterable[dict],
        entities: Set[Literal["case", "file"]] = frozenset(("case", "file")),
        include_fields: Iterable[str] | Literal[True] = True,
    ) -> sql.DataFrame:
        """
        Loads the primary aliquot data from elasticsearch into a dataframe including the
        aliquot level id.

        Args:
            filters: The filters to be included in the must claus of the bool query when
                loading the data from elasticsearch.
            entities: The entity over which to find a primary aliquot.
            include_fields: The fields which should be included when loading data from
                elasticsearch.

        Returns:
            The primary aliquot dataframe

            primary_aliquot {}
            |---aliquot_created_datetime
            |---aliquot_id
            |---case
            |---case_id
            |---created_datetime
            |---entity
            |---entity_id
            |---file_id
            |---sample_id
            +---*additional_selections
        """
        sample_include_fields: Iterable[str] | Literal[True] = (
            include_fields
            if include_fields is True
            else filter(
                lambda f: f not in ("aliquot_created_datetime", "aliquot_id"),
                include_fields,
            )
        )
        primary_aliquot_df = super()._get_primary_aliquot_df(
            filters, entities, sample_include_fields
        )

        aliquot_df = self._get_aliquot_level_df(filters)
        primary_aliquot_df = primary_aliquot_df.join(
            aliquot_df, on=["file_id", "case_id", "sample_id"], how="left"
        )

        aliquot_window = (
            sql.Window()
            .partitionBy("entity", "entity_id")
            .orderBy("aliquot_created_datetime", "aliquot_id")
        )

        return (
            primary_aliquot_df.withColumn("row_number", F.row_number().over(aliquot_window))
            .where(F.col("row_number") == 1)
            .select(
                "aliquot_created_datetime",
                "aliquot_id",
                "case",
                "case_id",
                "created_datetime",
                "entity",
                "entity_id",
                "file_id",
                "sample_id",
                "submitter_id",
                *self._additional_selections,
            )
        )


class ResourceBuilder[
    TResourceConfig: builders.ResourceBuilder,
    TInputDFs: Mapping[str, object],
](InputBuilder[TResourceConfig, TInputDFs]):
    def _schema(self) -> types.StructType:
        return schemas.load_schema(self._config.schema)

    def _load_resource_data(self) -> sql.DataFrame:
        with resources.as_file(
            resources.files(self._config.package).joinpath(self._config.resource)
        ) as p:
            df = self._spark_session.read.csv(
                p.as_uri(), schema=self._schema(), header=True, sep="\t", comment="#"
            )

        return df


def _walk_schema(field: types.StructField, child_name: str) -> types.StructField:
    """
    Walks the inputs fields data type field in order to find the child field with the
    input name.

    Args:
        field: the field found in a parent schema/struct type.
        child_name: the name of the desired child field.

    Returns:
        The child field with the given child_name.

    Raises:
        ValueError: this is raised if the input field is NOT a struct type, an array
            with an struct type for an element type, or a map type with a value type
            which is a struct type.
    """
    datatype = field.dataType

    while isinstance(datatype, (types.ArrayType, types.MapType)):
        if isinstance(datatype, types.ArrayType):
            datatype = datatype.elementType
        if isinstance(datatype, types.MapType):
            datatype = datatype.valueType

    if isinstance(datatype, types.StructType):
        return datatype[child_name]
    else:
        raise ValueError(
            f"Unexpected data type encountered while walking. DataType: {type(datatype)}"
        )


class IndexBuilder[TIndexConfig: builders.IndexBuilder, TInputDFs: Mapping[str, object]](
    InputBuilder[TIndexConfig, TInputDFs], abc.ABC
):
    """A builder base class for constructing data to be inserted into an elasticsearch index."""

    __slots__ = ("_es_dataframe_util", "_index_name", "_index_type", "_mappings_loader")

    def __init__(
        self,
        config: TIndexConfig,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        mappings_loader: es_utils.MappingsLoader,
        input_type: type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        super().__init__(config, spark_session, input_type, output)

        self._es_dataframe_util = es_dataframe_util
        self._mappings_loader = mappings_loader
        self._index_type = build.IndexType[self._output.name]
        self._index_name, _ = self._index_type.get_mappings_details()

    def _get_boolean_paths(self) -> Iterator[str]:
        """
        Find all the boolean field in mapping and return the paths

        Returns:
            An iterable of each path to a boolean field represented as a series of
            field names separated by a '.'.
        """

        def get_boolean_paths(node: esmodels.Properties, path: str = "") -> Iterator[str]:
            for key, value in node.items():
                subpath = f"{path}{key}"

                if value.get("type") == "boolean":
                    yield subpath
                elif "properties" in value:
                    yield from get_boolean_paths(value["properties"], f"{subpath}.")

        mappings = self._mappings_loader.load_mapper(self._index_type).mappings

        return get_boolean_paths(mappings.get("properties", {}))

    def _cast_booleans(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Ensure all the boolean fields in data frame are booleans before save to ES

        Args:
            df: pyspark dataframe to cast boolean

        Returns:
            the input dataframe with all boolean fields cast to such type.
        """
        paths = self._get_boolean_paths()
        schema = copy.deepcopy(df.schema)

        for raw_path in paths:
            path = iter(raw_path.split("."))
            fieldname = more_itertools.first(path)
            field = functools.reduce(_walk_schema, path, schema[fieldname])
            field.dataType = types.BooleanType()

        return df.select(*(F.col(f.name).cast(f.dataType) for f in schema.fields))

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        df = self._cast_booleans(df)
        df = super()._write(df)
        df = df.repartition(self._config.partition_size, self._config.id_field)

        logger.info(f"Writing to ES: {self.output.name}")
        self._es_dataframe_util.write(df, self._index_type, self._config.id_field)

        return df


class SQLiteBuilder[TConfig: builders.Builder, TInputDFs: Mapping[str, object]](
    InputBuilder[TConfig, TInputDFs], abc.ABC
):
    __slots__ = ("_database",)

    def __init__(
        self,
        config: TConfig,
        spark_session: sql.SparkSession,
        database: sqlite.SQLiteDatabase,
        input_type: type[TInputDFs],
        output: build.DataFrame,
    ) -> None:
        """A base builder for building and writing data to a SQLite database.

        Args:
            config: The configuration for the builder in this run.
            spark_session: The spark session associated with this run.
            database: The SQLite database to which the data should be written.
            input_type: The type of the input mapping which is expected as kwargs to the
                build method.
            output: The output data frame of this builder.
        """
        super().__init__(config, spark_session, input_type, output)

        self._database = database

    @property
    @abc.abstractmethod
    def _create(self) -> str:
        """The SQL command for creating the table in the database."""
        pass

    @property
    @abc.abstractmethod
    def _insert(self) -> str:
        """The SQL command for inserting values into the table in the database."""
        pass

    def _write(self, df: sql.DataFrame) -> sql.DataFrame:
        """Extends the base write by ALWAYS writing the data to the SQLite database.

        NOTE: See InputBuilder._write for base functionality.

        Args:
            df: The data frame containing the data built by this builder.

        Return:
            A data frame with the same data as the input data frame.
        """
        df = super()._write(df)

        logger.info(f"Writing: {self._output.name} to SQLite DB.")
        self._database.write(df, self._insert, self._create)

        return df
