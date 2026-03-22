from collections.abc import Sequence
from typing import TypedDict

from pyspark import sql

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.gene_expression import configuration


def _get_primary_aliquot_filters(projects: Sequence[str]) -> list[dict]:
    filters: list[dict] = [
        {"terms": {"data_type": ["Gene Expression Quantification"]}},
        # {"terms": {"acl": ["open"]}},
        {"terms": {"acl": ["*"]}},
        {"term": {"analysis.workflow_type": "STAR - Counts"}},
    ]

    if projects:
        project_filter = {
            "nested": {
                "path": "cases",
                "query": {"terms": {"cases.project.project_id": projects}},
            }
        }

        filters.append(project_filter)

    return filters


class PrimaryAliquotInputs(TypedDict):
    pass


class PrimaryAliquotBuilder(
    builders.PrimaryAliquotBuilder[configuration.PrimaryAliquotBuilder, PrimaryAliquotInputs]
):
    def __init__(
        self,
        config: configuration.PrimaryAliquotBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """
        Args:
            config: The app configuration object.
            spark_session: The spark session for the current pyspark run.
            es_dataframe_util: The util for creating dataframes from data in elasticsearch.
        """
        super().__init__(
            config,
            spark_session,
            es_dataframe_util=es_dataframe_util,
            input_type=PrimaryAliquotInputs,
            output=build.DataFrame.PRIMARY_ALIQUOT,
        )

    def _build_from_scratch(self, input_dfs: PrimaryAliquotInputs) -> sql.DataFrame:
        """
        Gets the case and it's associated file data for the mutation index.

        Args:
            input_dfs: The required input data frames to build the primary aliquots.

        Returns:
            A data frame containing the files and their associated cases selected by
            primary aliquot selection for the gene expression data.

            primary_aliquot {}
            |---file_id
            |---case_id
            +---submitter_id
        """
        filters = _get_primary_aliquot_filters(self._config.projects)
        case_fields = [
            "cases.submitter_id",
            "cases.samples.submitter_id",
        ]

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=case_fields,
        ).select(
            "file_id",
            "case_id",
            "case.submitter_id",
        )
