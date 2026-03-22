import abc
import logging
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer import builders, es_utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

logger = logging.getLogger(__name__)


AVAILABLE_VARIATION_DATA = "available_variation_data"


def _load_available_variation_data(
    maf_metadata_df: sql.DataFrame,
    # ascat_metadata_df: sql.DataFrame,
    gistic_metadata_df: sql.DataFrame,
    segment_cnv_metadata_df: sql.DataFrame,
) -> sql.DataFrame:
    ssm_data_df = maf_metadata_df.select(
        "case_id", F.lit("ssm").alias(AVAILABLE_VARIATION_DATA)
    ).distinct()
    # cnv_data_df = ascat_metadata_df.select(
    cnv_data_df = gistic_metadata_df.select(
        "case_id", F.lit("cnv").alias(AVAILABLE_VARIATION_DATA)
    ).distinct()
    segment_cnv_data_df = segment_cnv_metadata_df.select(
        "case_id", F.lit("segment_cnv").alias(AVAILABLE_VARIATION_DATA)
    ).distinct()
    available_variation_df = ssm_data_df.union(cnv_data_df).union(segment_cnv_data_df)

    # Finally, group by case
    return available_variation_df.groupby("case_id").agg(
        F.collect_set(AVAILABLE_VARIATION_DATA).alias(AVAILABLE_VARIATION_DATA)
    )


class CaseLoaderMixin(abc.ABC):
    """
    Builds a case dataframe by loading case documents from gdc_from_graph
    """

    @abc.abstractmethod
    def _load_es_case_data(self) -> sql.DataFrame:
        pass

    def _load_cases(
        self,
        maf_metadata_df: sql.DataFrame,
        # ascat_metadata_df: sql.DataFrame,
        gistic_metadata_df: sql.DataFrame,
        segment_cnv_metadata_df: sql.DataFrame,
        repartition_size: int,
    ) -> sql.DataFrame:
        """
        Builds Case dataframe
        """
        case_df = self._load_es_case_data()
        available_variation_df = _load_available_variation_data(
            # maf_metadata_df, ascat_metadata_df, segment_cnv_metadata_df
            maf_metadata_df, gistic_metadata_df, segment_cnv_metadata_df
        )

        case_df = case_df.join(available_variation_df, on=["case_id"], how="left")

        return case_df.repartition(repartition_size, "case_id")


class CaseInputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    # ascat_metadata_df: sql.DataFrame
    gistic_metadata_df: sql.DataFrame
    segment_cnv_metadata_df: sql.DataFrame


class CaseBuilder(
    builders.InputBuilder[configuration.CaseBuilder, CaseInputs], CaseLoaderMixin
):
    __slots__ = ("_es_dataframe_util", "_field_selector")

    def __init__(
        self,
        config: configuration.CaseBuilder,
        spark_session: sql.SparkSession,
        es_dataframe_util: es_utils.DataFrameUtil,
        field_selector: es_utils.CaseFieldSelector,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=CaseInputs, output=build.DataFrame.CASE
        )

        self._es_dataframe_util = es_dataframe_util
        self._field_selector = field_selector

    def _load_es_case_data(self) -> sql.DataFrame:
        if self._config.projects:
            query = {"query": {"terms": {"project.project_id": self._config.projects}}}
        else:
            query = {"query": {"match_all": {}}}

        fields = self._field_selector.select_for(
            build.IndexType.CASE,
            build.IndexType.CNV_CENTRIC,
            build.IndexType.CNV_OCCURRENCE_CENTRIC,
            build.IndexType.GENE_CENTRIC,
            build.IndexType.SEGMENT_CNV_CENTRIC,
            build.IndexType.SEGMENT_CNV_OCCURRENCE_CENTRIC,
            build.IndexType.SSM_CENTRIC,
            build.IndexType.SSM_OCCURRENCE_CENTRIC,
        )

        # Only retrieve the fields we want
        logger.debug(f"Included fields: {fields}")

        # Load cases from graph index
        return self._es_dataframe_util.read(
            build.IndexType.CASE,
            source_filter=fields,
            include_as_arrays=self._config.include_as_arrays,
            query=query,
        )

    def _build_from_scratch(self, input_dfs: CaseInputs) -> sql.DataFrame:
        return self._load_cases(
            input_dfs["maf_metadata_df"],
            # input_dfs["ascat_metadata_df"],
            input_dfs["gistic_metadata_df"],
            input_dfs["segment_cnv_metadata_df"],
            self._config.repartition_size,
        )
