import collections
import functools
import itertools
import json
from collections.abc import (
    Collection,
    Container,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Set,
)
from types import MappingProxyType
from typing import Final, Literal

import elasticsearch
import gdcmodels
import pyspark
from elasticsearch import helpers
from gdcmodels import esmodels, mapper
from pyspark import sql
from pyspark.sql import types

from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.constants import build


def iterate_es_results(
    es_client: elasticsearch.Elasticsearch,
    index_name: str,
    doc_type: str | None = None,
    query: dict | None = None,
) -> Iterable:
    """
    Returns iterator over elasticsearch query results
    """
    doc_iterator = helpers.scan(
        es_client,
        index=index_name,
        doc_type=doc_type,
        scroll="2m",
        size=100,
        query=query or {},
    )

    return doc_iterator


@functools.cache
def _load_models() -> Mapping[str, Mapping[str, mapper.ModelMapper]]:
    """
    A cached call to `gdcmodels.get_es_models`.

    Returns:
        The loaded model mappers from gdcmodels.
    """
    return gdcmodels.get_es_models(vestigial_included=False)


class MappingsLoader:
    """A class for loading the elasticsearch mapping for any given index."""

    __slots__ = ()

    def load_mapper(self, index_type: build.IndexType) -> mapper.ModelMapper:
        """
        Loads the mapping for the given index.

        Args:
            index_type: the index type associated with the elasticsearch mapping to
                load.

        Returns:
            A mappings dict based on the configured output of the given index type.
        """
        index_name, doc_type = index_type.get_mappings_details()

        return _load_models()[index_name][doc_type or index_name]


def _is_included_field(
    excluded_fields: Container[str],
    included_fields: Iterable[str] | None,
    field: str,
) -> bool:
    """
    Determines if the given field should be included in the returned values based on the
    given excluded and included fields.

    Args:
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        field: the field in question.

    Returns:
        True if the field is a valid field and should be included in the resulting set
        of fields.
    """
    if field in excluded_fields:
        return False

    return included_fields is None or any(
        field.startswith(prefix) for prefix in included_fields
    )


def _convert_properties(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Iterable[str] | None,
    path: str = "",
) -> Iterator[str]:
    """
    Converts all properties in the given mapping into flat fields which fall within the
    given included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path: the current path to the given set of properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    fields: Iterable[tuple[str, Mapping]] = (
        (f"{path}{prop}", details) for prop, details in properties.items()
    )
    is_included_field = functools.partial(_is_included_field, excluded_fields, included_fields)
    fields = filter(lambda items: is_included_field(items[0]), fields)

    for field, details in fields:
        if "properties" in details:
            yield from _convert_properties(
                details["properties"],
                excluded_fields,
                included_fields,
                path=f"{field}.",
            )
        else:
            yield field


def _extract_fields(
    properties: Mapping[str, Mapping],
    excluded_fields: Container[str],
    included_fields: Iterable[str] | None,
    path_to_fields: collections.deque[str],
) -> Iterator[str]:
    """
    Extracts all fields which fall under the provided path and fall within the given
    included fields as well as outside of the excluded fields.

    Args:
        properties: the properties node of a elasticsearch mapping
        excluded_fields: fields to excluded from the encountered otherwise valid fields.
            Beyond excluding specific fields, this can be used to exclude all children
            of a given property.
        included_fields: a sub set of fields to be included from the encountered fields.
            This is useful for retrieving all children of a given property.
        path_to_fields: a series of properties which represent the path to the desired
            fields found within the given properties.

    Yields:
        Individual fields from the given properties mapping.
    """
    if not properties:
        return

    if path_to_fields:
        next_prop = path_to_fields.popleft()
        properties = properties.get(next_prop, {}).get("properties", {})

        yield from _extract_fields(
            properties, excluded_fields, included_fields, path_to_fields
        )

    else:
        yield from _convert_properties(properties, excluded_fields, included_fields)


class CaseFieldSelector:
    """A class for selecting the case fields in a given elasticsearch index."""

    __slots__ = ("_mappings_loader",)

    CASE_PREFIXES: Final[Mapping[build.IndexType, str]] = MappingProxyType(
        {
            build.IndexType.CASE: "",
            build.IndexType.CASE_CENTRIC: "",
            build.IndexType.CNV_CENTRIC: "occurrence.case",
            build.IndexType.CNV_OCCURRENCE_CENTRIC: "case",
            build.IndexType.GENE_CENTRIC: "case",
            build.IndexType.SEGMENT_CNV_CENTRIC: "occurrence.case",
            build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC: "case",
            build.IndexType.SSM_CENTRIC: "occurrence.case",
            build.IndexType.SSM_OCCURRENCE_CENTRIC: "case",
        }
    )

    def __init__(self, mappings_loader: MappingsLoader | None = None) -> None:
        self._mappings_loader = mappings_loader or MappingsLoader()

    def _select_fields(
        self,
        index_type: build.IndexType,
        excluded_fields: Container[str],
        included_fields: Iterable[str] | None,
    ) -> Set[str]:
        if index_type not in self.CASE_PREFIXES:
            raise ValueError(f"Index: {index_type.name} is not supported.")

        prefix = self.CASE_PREFIXES[index_type]
        path_to_fields = (
            collections.deque(prefix.split(".")) if prefix else collections.deque()
        )
        mappings = self._mappings_loader.load_mapper(index_type).mappings
        fields = _extract_fields(
            mappings["properties"], excluded_fields, included_fields, path_to_fields
        )

        return frozenset(fields)

    def select_for(
        self,
        *index_types: build.IndexType,
        excluded_fields: Container[str] = (),
        included_fields: Iterable[str] | None = None,
    ) -> Set[str]:
        """
        Selects all common case fields found in the given indices.

        Args:
            *index_types: any indicies which should be included when selecting the case
                fields. Valid types: CASE_CENTRIC, CNV_CENTRIC, CNV_OCCURRENCE_CENTRIC,
                SEGMENT_CNV_CENTRIC, SSM_CENTRIC, and SSM_OCCURRENCE_CENTRIC
            excluded_fields: any fields which should be excluded in the selection. If
                a parent field is excluded then all of its children will be eg. if the
                exclusion is samples, then samples.sample_id is automatically excluded.
            included_fields: restricts the select to only included a subset of fields.
                this is useful when slecting fields nested under a particular parent.
                The default is to include all fields.
        """
        field_sets = (
            self._select_fields(index_type, excluded_fields, included_fields)
            for index_type in index_types
        )

        return functools.reduce(lambda set0, set1: set0 & set1, field_sets) | frozenset(
            ("case_id",)
        )


SQL_TYPES: Mapping[str, types.DataType] = MappingProxyType(
    {
        "boolean": types.BooleanType(),
        "double": types.DoubleType(),
        "float": types.DoubleType(),
        "integer": types.LongType(),
        "keyword": types.StringType(),
        "long": types.LongType(),
    }
)
"""A mapping of elasticsearch types to their associated SQL types."""


Tree = dict[str, "Tree"]


class DefaultTree(collections.defaultdict[str, Tree]):
    """A tree structure for which all paths are valid."""

    def __init__(self) -> None:
        super().__init__(default_factory=DefaultTree)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str)


def _parse_tree(paths: Iterable[str]) -> Tree:
    """Parses a tree structure based on the given paths.

    Example:
        paths:
            - samples.sample_id
            - samples.aliquots.aliquot_id
        return:
            {
                "samples": {
                    "sample_id": {},
                    "aliquots": {
                        "aliquot_id": {}
                    }
                }
            }

    Args:
        paths: The paths representing the path to elements in a tree structure.

    Returns:
        A tree structure of the various paths described in the input.
    """
    tree: Tree = {}

    for path in (p.split(".") for p in paths):
        functools.reduce(lambda a, e: a.setdefault(e, {}), path, tree)

    return tree


def _walk_struct(struct: types.StructType, path: Sequence[str]) -> types.StructField | None:
    """Walks the provided path within the given SQL structure.

    Args:
        struct: The root structure at the origin of the given path.
        path: A series of field names which represent the path to the desired field.

    Returns:
        The structure field found at the end of the path if the path exists; otherwise
        `None`.
    """

    def get_field(struct: types.StructType, field_name: str) -> types.StructField | None:
        """Gets the field with the given name if it exists.

        Args:
            struct: The structure which should contain the field.
            field_name: The name of the field in the structure.

        Returns:
            The field with the given name; otherwise `None`.
        """
        if field_name in struct.fieldNames():
            return struct[field_name]

        return None

    def walk_struct(struct: types.StructType, field_name: str) -> types.StructType:
        """Walks to the given field assuming it is a substructure.

        Args:
            struct: The structure to be traversed.
            field_name: The name of the field containing the substructure.

        Returns:
            The substructure contained in the in the field specified. If the field
            either does not exist or is not a `StructType` then a dummy structure
            containing no fields is returned.
        """
        field = get_field(struct, field_name)

        if not field:
            return types.StructType([])

        data_type = field.dataType

        while isinstance(data_type, types.ArrayType):
            data_type = data_type.elementType

        if isinstance(data_type, types.StructType):
            return data_type

        return types.StructType([])

    assert len(path) >= 1, "Path must have at least one element"

    parent = functools.reduce(walk_struct, path[:-1], struct)

    return get_field(parent, path[-1])


class SchemaLoader:
    """A class for loading a schema for a data frame based on an elasticsearch mapping."""

    def _convert_properties(
        self, properties: esmodels.Properties, included: Tree
    ) -> Iterator[types.StructField]:
        """Converts a set of properties to their equivalent SQL structures.

        Args:
            properties: The set of properties that need to be converted.
            included: A tree of representing the properties to included in this
                conversion. All keys represent the current properties which should be
                included and their values represent any properties in a substructure
                which should also be included.

        Yields:
            The converted fields which belong to a parent structure.
        """
        for name, details in properties.items():
            if name not in included:
                continue
            elif "properties" in details:
                fields = self._convert_properties(details["properties"], included[name])

                yield types.StructField(name, types.StructType(list(fields)))
            elif "type" in details and details["type"] in SQL_TYPES:
                yield types.StructField(name, SQL_TYPES[details["type"]])
            else:
                raise ValueError(f"Invalid Property: {name}({details})")

    def _convert_to_arrays(
        self, struct: types.StructType, include_as_arrays: Iterable[str]
    ) -> None:
        """Converts the fields found at the given array paths in the struct to arrays.

        Args:
            struct: The structure which needs needs the elements at the given array
                paths to be converted to ArrayTypes.
            include_as_arrays: The property paths which will be arrays in the resulting
                data from elasticsearch.
        """
        paths = (a.split(".") for a in include_as_arrays)
        array_fields = (_walk_struct(struct, p) for p in paths)
        array_fields = filter(None, array_fields)

        for field in array_fields:
            field.dataType = types.ArrayType(field.dataType)

    def load(
        self,
        mappings: esmodels.ESMapping,
        source_filter: Literal[True] | Iterable[str],
        include_as_arrays: Iterable[str],
    ) -> types.StructType:
        """Loads the schema from the mappings.

        Args:
            mappings: The elasticsearch mapping on which the resulting schema is based.
            source_filter: The source properties which are the only ones that should be
                included in the resulting schema.
            include_as_arrays: As the mapping does not convey which properties are
                arrays, this is a list of fields which need to be converted to array
                types.

        Returns:
            The schema of the data that will be loaded from the given mapping with the
            given source_filter & include_as_arrays applied.
        """
        included = DefaultTree() if source_filter is True else _parse_tree(source_filter)
        struct = types.StructType(
            list(self._convert_properties(mappings["properties"], included))
        )
        # Always ensure that nested fields are included as arrays.
        include_as_arrays = frozenset(
            itertools.chain(include_as_arrays, _get_nested_document_properties(mappings))
        )

        self._convert_to_arrays(struct, include_as_arrays)

        return struct


def _get_index(config: es_config.Elasticsearch, index_type: build.IndexType) -> str:
    if index_type == build.IndexType.FILE:
        return config.read.file_index

    if index_type == build.IndexType.CASE:
        return config.read.case_index

    if index_type in config.write.indices:
        return config.write.indices[index_type]

    raise ValueError(f"Index not configured: {index_type.name}")


def _get_nested_document_properties(mappings: esmodels.ESMapping) -> frozenset[str]:
    """Gets all properties from the mapping with the nested type.

    Args:
        mappings: The mapping in which the nested document properties are located.

    Returns:
        A frozenset of all properties which nested documents.
    """

    def scan_properties(
        properties: esmodels.Properties, path: Iterable[str] = ()
    ) -> Iterator[str]:
        """Scans properties for any with a nested types.

        This functionality recurses through any properties which contain a substructure.

        Args:
            properties: The properties to scan for nested documents.
            path: The path traversed so far to find these properties.

        Yields:
            The path to any nested documents.
        """
        for name, details in properties.items():
            property_path = (*path, name)

            if details.get("type") == "nested":
                yield ".".join(property_path)

            yield from scan_properties(details.get("properties", {}), property_path)

    return frozenset(scan_properties(mappings["properties"]))


class DataFrameUtil:
    __slots__ = (
        "_config",
        "_es_client",
        "_mappings_loader",
        "_schema_loader",
        "_spark_session",
    )
    ES_FORMAT = "org.elasticsearch.spark.sql"

    def __init__(
        self,
        config: es_config.Elasticsearch,
        spark_session: sql.SparkSession,
        es_client: elasticsearch.Elasticsearch,
        mappings_loader: MappingsLoader,
        schema_loader: SchemaLoader,
    ) -> None:
        self._config = config
        self._spark_session = spark_session
        self._es_client = es_client
        self._mappings_loader = mappings_loader
        self._schema_loader = schema_loader

    def _get_index(self, index_type: build.IndexType) -> str:
        return _get_index(self._config, index_type)

    def read(
        self,
        index_type: build.IndexType,
        source_filter: Literal[True] | Collection[str] = True,
        include_as_arrays: Iterable[str] = (),
        query: dict | None = None,
        read_metadata: bool = False,
    ) -> sql.DataFrame:
        """
        A utility for reading data from ES natively into a spark data frame.

        Args:
            index_type: The index from which the data will be loaded
            source_filter: The properties to which this query should be restricted. True
                means all properties should be included.
            include_as_arrays: The fields which need to be read as arrays and not
                simple types (e.g. field: ["these", "are", "values"])
                NOTE: Nested documents will automatically be accounted for.
            query: The query to use in ES to limit the records returned

        Returns:
            A data frame containing the data from the elasticsearch index
        """
        mappings = self._mappings_loader.load_mapper(index_type).mappings
        # We need to insure that all nested documents are included as arrays.
        include_as_arrays = _get_nested_document_properties(mappings).union(include_as_arrays)
        source_schema = self._schema_loader.load(mappings, source_filter, include_as_arrays)
        schema = types.StructType(
            [
                types.StructField("_id", types.StringType()),
                types.StructField("_source", source_schema),
            ]
        )
        config = {
            "es.read.metadata": str(read_metadata),
            "es.nodes": self._config.connection.nodes,
            "es.net.http.auth.user": self._config.connection.user,
            "es.net.http.auth.pass": self._config.connection.password,
            "es.net.ssl": str(self._config.connection.use_ssl),
            "es.net.ssl.cert.allow.self.signed": str(not self._config.connection.verify_certs),
            "es.nodes.resolve.hostname": str(False),
            "es.resource": _get_index(self._config, index_type),
        }

        if query:
            config["es.query"] = json.dumps(query)

        if isinstance(source_filter, Collection):
            # In order to filter the resulting data, we should preferably use the config
            # `es.read.source.filter`. This will use the elasticsearch `_source` field
            # when querying the data and insure that only the bare minimum data is
            # communicated over the network. However, when the number of properties in
            # the filter get too numerous, then this will cause a memory issue and cause
            # the query to fail in elasticsearch. Hence, in these cases, we should use
            # `es.read.field.include` which will ensure the executor only includes the
            # given fields when reading the data from elasticsearch.
            if len(source_filter) <= self._config.read.max_source_filter_length:
                config["es.read.source.filter"] = ",".join(source_filter)
            else:
                config["es.read.field.include"] = ",".join(source_filter)

        if include_as_arrays and isinstance(include_as_arrays, Iterable):
            config["es.read.field.as.array.include"] = ",".join(include_as_arrays)

        return (
            self._spark_session.sparkContext.newAPIHadoopRDD(
                "org.elasticsearch.hadoop.mr.EsInputFormat",
                "org.apache.hadoop.io.NullWritable",
                "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=config,
            )
            .toDF(schema=schema)
            .select("_source.*")
        )

    def _create_index(self, index: str, index_type: build.IndexType) -> None:
        """
        Creates the index based on the mapping associated with the given index
        type.

        Args:
            index: the name of the index to be created
            index_type: the index type correlating to the mapping for the new index
        """
        if self._es_client.indices.exists(index=index):
            raise Exception(f"Index: {index} already exists. Cannot overwrite existing index.")

        mappings = self._mappings_loader.load_mapper(index_type)

        self._es_client.indices.create(
            index=index, mappings=mappings.mappings, settings=mappings.settings
        )

    def write(self, df: sql.DataFrame, index_type: build.IndexType, id_field: str) -> None:
        """
        A utility for writing data from a data frame into elasticsearch.

        Args:
            df: the data frame which will be writen to elasticsearch for indexing
            index_type: the index type i.e. ssm_centric_index which the data will be
                written to
        """
        index = self._get_index(index_type)

        self._create_index(index, index_type)
        (
            df.write.format(self.ES_FORMAT)
            .option("es.nodes", self._config.connection.nodes)
            .option("es.net.http.auth.user", self._config.connection.user)
            .option("es.net.http.auth.pass", self._config.connection.password)
            .option("es.net.ssl", self._config.connection.use_ssl)
            .option(
                "es.net.ssl.cert.allow.self.signed",
                not self._config.connection.verify_certs,
            )
            .option("es.nodes.wan.only", "true")
            .option("es.nodes.resolve.hostname", "false")
            .option("es.resource.write", index)
            .option("es.http.timeout", "1h")
            .option("es.http.retries", "-1")
            .option("es.batch.write.retry.count", "-1")
            .option("es.batch.write.retry.wait", "10m")
            .option("es.batch.size.bytes", self._config.write.batch_size_bytes)
            .option("es.batch.size.entries", self._config.write.batch_size_entries)
            .option("es.batch.write.refresh", True)
            .option("es.mapping.id", id_field)
            .save(index)
        )


class RDDUtil:
    """
    A tool for loading spark RDD containing data loaded from an  elasticsearch index.

    CAUTION: In any case where the data has a regular schema and thus can be loaded
        using the DataframeUtil, default to loading the optimizable DataFrame object
        vs an RDD.
    """

    __slots__ = ("_config", "_spark_context")

    def __init__(
        self, config: es_config.Elasticsearch, spark_context: pyspark.SparkContext
    ) -> None:
        self._config = config
        self._spark_context = spark_context

    def _get_index(self, index_type: build.IndexType) -> str:
        return _get_index(self._config, index_type)

    def get_rdd(
        self,
        index_type: build.IndexType,
        include_fields: Iterable[str] | bool = True,
        exclude_fields: Iterable[str] | None = None,
        include_as_arrays: Iterable[str] = (),
        exclude_as_arrays: Iterable[str] = (),
        query: dict | None = None,
        read_metadata: bool = False,
    ) -> pyspark.RDD:
        """
        A utility for loading data from ES natively into spark RDD objects.

        CAUTION: Use this only for loading data which cannot conform to a schema
            and thus cannot be loaded into a dataframe. All RDD objects should be
            standardized and converted into dataframes with `.toDF(SCHEMA)` ASAP in
            the process to maximize optimization of the spark program.

        Args:
            index: The index from which the data will be loaded
            include_fields: The fields which will be included when read
            include_as_arrays: The fields which need to be read as arrays and not
                simple types (e.g. field: ["this", "is", "example"])
                NOTE: This does NOT apply to arrays of objects
            query: The query to use in ES to limit the records returned

        Returns:
            An RDD dataset which contains the dynamic data returned by the query
            to the provided elasticsearch index.
        """
        config = {
            "es.read.metadata": str(read_metadata),
            "es.nodes": self._config.connection.nodes,
            "es.net.http.auth.user": self._config.connection.user,
            "es.net.http.auth.pass": self._config.connection.password,
            "es.net.ssl": str(self._config.connection.use_ssl),
            "es.net.ssl.cert.allow.self.signed": str(not self._config.connection.verify_certs),
            "es.nodes.resolve.hostname": str(False),
            "es.resource": self._get_index(index_type),
        }

        if query:
            config["es.query"] = json.dumps(query)

        if include_fields and isinstance(include_fields, Iterable):
            config["es.read.field.include"] = ",".join(include_fields)

        if exclude_fields and isinstance(exclude_fields, Iterable):
            config["es.read.field.exclude"] = ",".join(exclude_fields)

        if include_as_arrays and isinstance(include_as_arrays, Iterable):
            config["es.read.field.as.array.include"] = ",".join(include_as_arrays)

        if exclude_as_arrays and isinstance(exclude_as_arrays, Iterable):
            config["es.read.field.as.array.exclude"] = ",".join(exclude_as_arrays)

        return self._spark_context.newAPIHadoopRDD(
            "org.elasticsearch.hadoop.mr.EsInputFormat",
            "org.apache.hadoop.io.NullWritable",
            "org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=config,
        )
