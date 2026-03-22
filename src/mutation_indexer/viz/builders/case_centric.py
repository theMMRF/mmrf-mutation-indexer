from typing import Self

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import es_utils
from mutation_indexer.configuration import adapter
from mutation_indexer.constants import build
from mutation_indexer.viz.builders import (
    base_builder,
    case,
    consequence,
    df_builders,
    observation,
)

SEGMENT_CNV_COLUMNS = (
    "segment_cnv_id",
    "chromosome",
    "length",
    "start_position",
    "end_position",
    "cnv_change",
    "cnv_change_5_category",
)


class CaseCentricBuilder(base_builder.BaseBuilder, case.CaseLoaderMixin):
    """
    Builds case-centric dataframe given case, maf, and segment_cnv dataframes:

        case{}
            |___ gene[]
            |        |___ ssm[]
            |        |     |___ consequence[]
            |        |     |             |_____ transcript{}
            |        |     |                          |_____ annotation{}
            |        |     |___ observation[]
            |        |
            |        |___ cnv[]
            |              |___ consequence[]
            |              |            |_____ gene{}
            |              |
            |              |___ observation[]
            |___ segment_cnv[]
                     |____ observation[]
    """

    index_name = "case_centric"
    id_field = "case_id"

    def __init__(
        self,
        config: adapter.ObsoleteConfig,
        sql_context: sql.SQLContext,
        es_dataframe_util: es_utils.DataFrameUtil,
        field_selector: es_utils.CaseFieldSelector,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ):
        super().__init__(config, sql_context)

        self._es_dataframe_util = es_dataframe_util
        self._field_selector = field_selector

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def _load_es_case_data(self) -> sql.DataFrame:
        if (
            False and self.config.projects
        ):  # TODO: DEV-1256 Restore func w/ new config specific projects
            query = {"query": {"terms": {"project.project_id": self.config.projects}}}
        else:
            query: dict = {"query": {"match_all": {}}}

        fields = self._field_selector.select_for(
            build.IndexType.CASE,
            build.IndexType.CASE_CENTRIC,
        )

        self.logger.info(f"Included case fields: {fields}")

        return self._es_dataframe_util.read(
            build.IndexType.CASE,
            source_filter=fields,
            include_as_arrays=self.config.case_include_as_arrays,
            query=query,
        )

    def _build_segment_cnv_subtree(self, segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
        """Aggregates all segment_cnvs for each case.

        segment_cnv_subtree{}
            |____ case_id
            |____ segment_cnv []
                    |____ observation[]

        STEPS:
            1) Select the pertinent segment_cnv columns.

            2) Build the observation dataframe, which will collect all observations for
            each segment_cnv_id and case_id combination.

            3) Join the observation dataframe with a filtered segment_cnv_df. Before the
            join, we want to drop duplicate rows based on segment_cnv_id to reduce
            amount of work.

            4) Aggregate all segment_cnvs from step 3 and create a list of segment_cnvs
            associated with each case. The case_id is required to be able to join back to
            the final case_centric dataframe.
        """
        obs_df = observation.build_observation_for_segment_cnv(segment_cnv_df).select(
            "segment_cnv_id", "case_id", "observation"
        )
        segment_cnv_df = segment_cnv_df.select(*SEGMENT_CNV_COLUMNS).distinct()
        subtree_df = segment_cnv_df.join(obs_df, on="segment_cnv_id", how="inner").select(
            F.struct(*SEGMENT_CNV_COLUMNS, "observation").alias("segment_cnv"),
            "segment_cnv_id",
            "case_id",
        )
        subtree_df = subtree_df.groupBy("case_id").agg(
            F.collect_set("segment_cnv").alias("segment_cnv")
        )
        subtree_df = subtree_df.select("case_id", "segment_cnv")

        return subtree_df

    def build(
        self,
        maf_metadata_df: sql.DataFrame,
        maf_df: sql.DataFrame,
        # ascat_metadata_df: sql.DataFrame,
        # ascat_df: sql.DataFrame,
        gistic_metadata_df: sql.DataFrame,
        gistic_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        segment_cnv_df: sql.DataFrame,
        segment_cnv_metadata_df: sql.DataFrame,
        **kwargs: sql.DataFrame,
    ) -> Self:
        """
        Builds Case Centric index
        """
        self.log("Building CaseCentric")
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "load":
            self.case_centric = self.load_raw()
            if self.case_centric is not None:
                return self

        case_df = self._load_cases(
            maf_metadata_df,
            # ascat_metadata_df,
            gistic_metadata_df,
            segment_cnv_metadata_df,
            self.config.df_repartition,
        )

        self.log("Building Gene subtree")
        # gene_subtree = self.build_gene_subtree(maf_df, ascat_df, primary_aliquot_df)
        gene_subtree = self.build_gene_subtree(maf_df, gistic_df, primary_aliquot_df)

        self.log("Join Case with Gene subtree [left, case_id]")
        case_centric = case_df.join(gene_subtree, on=["case_id"], how="left")
        self.log_count(case_centric)

        self.log("Building Segment CNV subtree")
        segment_cnv_subtree = self._build_segment_cnv_subtree(segment_cnv_df)

        self.log("Join Case with Segment CNV subtree [left, case_id]")
        case_centric = case_centric.join(segment_cnv_subtree, on=["case_id"], how="left")

        self.log("Finalizing case_centric build")
        case_centric = self._final_transform(case_centric)
        self.log_count(case_centric)

        self.case_centric = case_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_gene_subtree(
        self,
        maf_df: sql.DataFrame,
        # ascat_df: sql.DataFrame,
        gistic_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """

        # TODO Refactor with gene centric.
        # self.log("Building Gene from MAF and ASCAT")
        self.log("Building Gene from MAF and GISTIC")
        gene_df = df_builders.get_gene_df(
            maf_df,
            self.index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )

        # ascat_gene_df = df_builders.get_gene_df(
        gistic_gene_df = df_builders.get_gene_df(
            # ascat_df,
            gistic_df,
            self.index_name,
            add_fields=["case_id"],
            drop_fields=[
                "canonical_transcript_length",
                "canonical_transcript_length_cds",
                "canonical_transcript_length_genomic",
            ],
        )

        # gene_df = gene_df.union(ascat_gene_df).distinct()
        gene_df = gene_df.union(gistic_gene_df).distinct()
        self.log_count(gene_df)

        self.log("Building SSM subtree")
        ssm_df = self.build_ssm_subtree(maf_df, primary_aliquot_df)
        self.log_count(ssm_df)

        self.log("Building CNV subtree")
        # cnv_df = self.build_cnv_subtree(ascat_df)
        cnv_df = self.build_cnv_subtree(gistic_df)
        self.log_count(cnv_df)

        self.log("Join SSM and CNV subtrees to Gene [left, gene_id, case_id]")
        gene_ssm_cnv_df = gene_df.join(ssm_df, on=["gene_id", "case_id"], how="left").join(
            cnv_df, on=["gene_id", "case_id"], how="left"
        )
        self.log_count(gene_ssm_cnv_df)

        self.log("Grouping SSM and CNV subtrees under Gene")
        gene_ssm_cnv_df = gene_ssm_cnv_df.select(
            "case_id",
            F.struct("ssm", "cnv", *gene_df.drop("case_id").columns).alias("gene"),
        )
        self.log_count(gene_df)

        self.log('Grouping by case_id and aggregating to list under "gene"')
        gene_ssm_cnv_df = gene_ssm_cnv_df.groupBy(gene_ssm_cnv_df.case_id).agg(
            F.collect_list("gene").alias("gene")
        )
        return gene_ssm_cnv_df

    def build_ssm_subtree(
        self, maf_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """
        # Consequence
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df, self.index_name, join_gene=False
        )

        # Observation
        obs_df = self.observation_builder.build_for_ssm(
            maf_df,
            primary_aliquot_df,
            self.index_name,
            selector="ssm",
        )
        obs_df = obs_df.drop("occurrence_id")

        # SSM
        ssm_df = df_builders.build_ssm_subtree(maf_df, cons_df, self.index_name, obs_df=obs_df)

        # Aggregate SSM
        self.log("Aggregating ssm by case_id and gene_id")
        ssm_df = (
            ssm_df.select(
                "gene_id",
                "case_id",
                F.struct(*ssm_df.drop("gene_id").drop("case_id").columns).alias("ssm"),
            )
            .groupBy(["gene_id", "case_id"])
            .agg(F.collect_list("ssm").alias("ssm"))
        )

        return ssm_df

    # def build_cnv_subtree(self, ascat_df):
    def build_cnv_subtree(self, gistic_df):
        """
        cnv[]
           |___ observation[]

        """

        # Observation
        obs_df = self.observation_builder.build_for_cnv(
            # ascat_df,
            gistic_df,
            self.index_name,
            selector="cnv",
        )

        # Build the final cnv dataframe
        # cnv_df = df_builders.build_cnv_subtree(ascat_df, self.index_name, obs_df=obs_df)
        cnv_df = df_builders.build_cnv_subtree(gistic_df, self.index_name, obs_df=obs_df)

        # Aggregate CNV
        self.log("Aggregating cnv by case_id and gene_id")
        cnv_df = (
            cnv_df.select(
                "gene_id",
                "case_id",
                F.struct(*cnv_df.drop("gene_id").drop("case_id").columns).alias("cnv"),
            )
            .groupBy(["gene_id", "case_id"])
            .agg(F.collect_list("cnv").alias("cnv"))
        )

        return cnv_df

    def _final_transform(self, case_centric):
        """
        Final case_centric dataframe transformation:

        - add 'available_variation_data'
        - truncate outliers
        """
        # Coerce any cases that didn't have variation data from None to []
        case_centric = case_centric.withColumn(
            "available_variation_data",
            F.udf(lambda x: [] if (x is None) else x, types.ArrayType(types.StringType()))(
                F.col("available_variation_data")
            ),
        )

        # Truncate outliers
        threshold = self.config.percentile_threshold["genes_per_case"]
        case_centric = self.truncate_df_at_percentile(case_centric, "gene", threshold)

        return case_centric
