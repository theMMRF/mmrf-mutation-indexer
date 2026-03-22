from typing import TypedDict

from pyspark import sql

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration


class PrimaryAliquotInputs(TypedDict):
    pass


class PrimaryAliquotBuilder(
    builders.InclusivePrimaryAliquotBuilder[
        configuration.PrimaryAliquotBuilder, PrimaryAliquotInputs
    ]
):
    __slots__ = ("_es_rdd_util",)

    FILE_URL_BATCH_SIZE = 1000

    def __init__(
        self,
        config: configuration.PrimaryAliquotBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object
            sql_context: The sql context object for the current pyspark run
            es_dataframe_util: The util for creating dataframes from data in elasticsearch
            es_rdd_util: The util for creating RDD objects from data in elasticsearch
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            es_rdd_util,
            input_type=PrimaryAliquotInputs,
            output=build.DataFrame.PRIMARY_ALIQUOT,
            additional_selections=("experimental_strategy",),
        )
        self._es_rdd_util = es_rdd_util

    def _build_from_scratch(self, input_dfs: PrimaryAliquotInputs) -> sql.DataFrame:
        """
        Gets the file data associated with the best match sample for every
        case in the current processes configured project(s)

        Return:
            A data frame with the file data

            primary_aliquot{}
            |---aliquot_id
            |---case_id
            |---entity
            |---entity_id
            |---experimental_strategy
            +---file_id
        """
        filters = (
            [
                {
                    "nested": {
                        "path": "cases",
                        "query": {
                            "terms": {"cases.project.project_id": self._config.projects}
                        },
                    }
                }
            ]
            if self._config.projects
            else [{"match_all": {}}]
        )
        include_fields = (
            "experimental_strategy",
            "cases.samples.submitter_id",
        )

        return self._get_primary_aliquot_df(
            filters, entities=frozenset(("case", "file")), include_fields=include_fields
        ).select(
            "aliquot_id",
            "case_id",
            "entity",
            "entity_id",
            "experimental_strategy",
            "file_id",
        )
