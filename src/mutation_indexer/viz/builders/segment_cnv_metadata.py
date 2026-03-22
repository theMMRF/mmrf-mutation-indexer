from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build, datamodel
from mutation_indexer.viz import configuration


class SegmentCNVMetadataInputs(TypedDict):
    # ascat_metadata_df: sql.DataFrame
    gistic_metadata_df: sql.DataFrame


class SegmentCNVMetadataBuilder(
    builders.InputBuilder[configuration.SegmentCNVMetadataBuilder, SegmentCNVMetadataInputs]
):
    def __init__(
        self,
        config: configuration.SegmentCNVMetadataBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
    ) -> None:
        """Input dataframe builder that retrieves copy number segment files.

        Then, it uses the output of GISTICMetadataBuilder and joins the copy number
        segment files with the primary aliquot on analysis_id. This ensures that the
        copy number segment file will be the sibling file of the gene-level copy
        number file.
        """
        super().__init__(
            config,
            spark_session,
            input_type=SegmentCNVMetadataInputs,
            output=build.DataFrame.SEGMENT_CNV_METADATA,
        )

        self._es_dataframe_util = es_dataframe_util

    def _get_es_query(self) -> dict:
        acl_clause = {"terms": {"acl": self._config.acl}}
        allele_specific_cns_clause = {
            "bool": {
                "must": [
                    {
                        "term": {
                            "data_type": datamodel.DataType.ALLELE_SPECIFIC_COPY_NUMBER_SEGMENT
                        }
                    }
                ],
                "must_not": [
                    {"term": {"analysis.workflow_type": datamodel.WorkflowType.GATK4_CNV}}
                ],
            }
        }

        # # TODO DEV-3360: remove deprecated query conditional logic
        # if self._config.use_deprecated_query is True:
        #     deprecated_clause = {
        #         "bool": {
        #             "must": [
        #                 {"term": {"data_type": datamodel.DataType.COPY_NUMBER_SEGMENT}},
        #                 {"term": {"analysis.workflow_type": datamodel.WorkflowType.ASCAT_NGS}},
        #             ]
        #         }
        #     }

        #     return {
        #         "query": {
        #             "bool": {
        #                 "must": [acl_clause],
        #                 "should": [allele_specific_cns_clause, deprecated_clause],
        #                 "minimum_should_match": 1,
        #             }
        #         },
        #     }

        return {"query": {"bool": {"must": [acl_clause, allele_specific_cns_clause]}}}

    def _get_es_source_fields(self) -> tuple[str, ...]:
        return ("file_id", "analysis.analysis_id")

    def _build_from_scratch(self, input_dfs: SegmentCNVMetadataInputs) -> sql.DataFrame:
        """Builds the SegmentCNVMetadata dataframe.

        segment_cnv_metadata {}
        |---aliquot_id
        |---analysis_id
        |---case_id
        |---file_id
        |---workflow_type
        """
        # ascat_metadata_df = input_dfs["ascat_metadata_df"].select(
        gistic_metadata_df = input_dfs["gistic_metadata_df"].select(
            "aliquot_id", "analysis_id", "case_id", "workflow_type"
        )
        segment_cnv_metadata_df = self._es_dataframe_util.read(
            build.IndexType.FILE,
            source_filter=self._get_es_source_fields(),
            query=self._get_es_query(),
        )
        segment_cnv_metadata_df = segment_cnv_metadata_df.select(
            "file_id", F.col("analysis.analysis_id").alias("analysis_id")
        )
        # segment_cnv_metadata_df = ascat_metadata_df.join(
        segment_cnv_metadata_df = gistic_metadata_df.join(
            segment_cnv_metadata_df, on="analysis_id", how="inner"
        )
        segment_cnv_metadata_df = segment_cnv_metadata_df.select(
            "aliquot_id", "analysis_id", "case_id", "file_id", "workflow_type"
        )

        return segment_cnv_metadata_df
