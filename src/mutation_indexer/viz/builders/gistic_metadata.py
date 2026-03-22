from collections.abc import Collection, Sequence
from typing import Literal, TypedDict, override

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

import logging
logger = logging.getLogger(__name__)


class GISTICMetadataInputs(TypedDict):
    pass


class GISTICMetadataBuilder(
    builders.InclusivePrimaryAliquotBuilder[
        configuration.GISTICMetadataBuilder, GISTICMetadataInputs
    ]
):
    """
    A class for resolving the document IDs associated with the GISTIC documents in
    ES/Indexd.
    """

    def __init__(
        self,
        config: configuration.GISTICMetadataBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        es_rdd_util: es_utils.RDDUtil,
    ) -> None:
        super().__init__(
            config,
            spark_session,
            es_dataframe_util,
            es_rdd_util,
            additional_selections=(
                "workflow_type",
                "analysis_id",
                "experimental_strategy",
            ),
            input_type=GISTICMetadataInputs,
            output=build.DataFrame.GISTIC_METADATA,
        )

    @override
    def _weight_matrix(self) -> Sequence[Sequence[sql.Column]]:
        workflow_type = F.col("workflow_type")
        experimental_strategy = F.col("experimental_strategy")

        file_weights = tuple(
            (workflow_type == F.lit(p.workflow_type))
            & (experimental_strategy == F.lit(p.experimental_strategy))
            for p in self._config.priorities
        )

        # apply file weights as a higher order weight to the defaults.
        return (*super()._weight_matrix(), file_weights)

    @override
    def _get_initial_weighted_df(
        self, query: dict, include_fields: Collection[str] | Literal[True]
    ) -> sql.DataFrame:
        return (
            super()
            ._get_initial_weighted_df(query, include_fields)
            .select(
                "*",
                F.col("analysis.workflow_type").alias("workflow_type"),
                F.col("analysis.analysis_id").alias("analysis_id"),
            )
        )

    def _get_filters(self) -> list[dict]:
        return [
            {
                "bool": {
                    "must": [
                        {"term": {"data_type": "Gene Level Copy Number"}},
                        {"terms": {"acl": self._config.acl}},
                    ],
                    "minimum_should_match": 1,
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "term": {
                                            "experimental_strategy": p.experimental_strategy
                                        }
                                    },
                                    {"term": {"analysis.workflow_type": p.workflow_type}},
                                ]
                            },
                        }
                        for p in self._config.priorities
                    ],
                }
            }
        ]

    def _build_from_scratch(self, input_dfs: GISTICMetadataInputs) -> sql.DataFrame:
        filters = self._get_filters()

        return self._get_primary_aliquot_df(
            filters,
            entities=frozenset(("case",)),
            include_fields=(
                "analysis.workflow_type",
                "analysis.analysis_id",
                "experimental_strategy",
                "cases.samples.submitter_id",
            ),
        ).select(
            "aliquot_created_datetime",
            "aliquot_id",
            "case",
            "case_id",
            "created_datetime",
            "entity",
            "entity_id",
            "file_id",
            "sample_id",
            "workflow_type",
            "analysis_id",
            "experimental_strategy",
            F.explode("case.samples").alias("sample"),
        ).select(
            "aliquot_created_datetime",
            "aliquot_id",
            "case",
            "case_id",
            "created_datetime",
            "entity",
            "entity_id",
            "file_id",
            "sample_id",
            "workflow_type",
            "analysis_id",
            "experimental_strategy",
            F.col("sample.submitter_id").alias("submitter_id")
        ).select("aliquot_id", "case_id", "file_id", "sample_id", "workflow_type", "analysis_id", "submitter_id")
