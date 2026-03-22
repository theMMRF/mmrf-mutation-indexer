from typing import Self

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.configuration import adapter
from mutation_indexer.viz.builders import (
    base_builder,
    consequence,
    df_builders,
    observation,
)


class GeneCentricBuilder(base_builder.BaseBuilder):
    """
    Builds gene-centric dataframe given case and maf dataframes::

        gene{}
             |___ case[]
                     |___ ssm[]
                     |     |___ consequence[]
                     |     |             |_____ transcript{}
                     |     |                          |_____ annotation{}
                     |     |___ observation[]
                     |
                     |___ cnv[]
                           |___ consequence[]
                           |            |_____ gene{}
                           |
                           |___ observation[]
    """

    index_name = "gene_centric"
    id_field = "gene_id"

    def __init__(
        self,
        config: adapter.ObsoleteConfig,
        sql_context: sql.SQLContext,
        consequence_builder: consequence.ConsequenceBuilder,
        observation_builder: observation.ObservationBuilder,
    ):
        super().__init__(config, sql_context)

        self.consequence_builder = consequence_builder
        self.observation_builder = observation_builder

    def build(
        self,
        maf_df: sql.DataFrame,
        # ascat_df: sql.DataFrame,
        gistic_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        **kwargs: sql.DataFrame,
    ) -> Self:
        """
        Builds Gene Centric index
        """
        self.log("Building GeneCentric")
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "read":
            self.gene_centric = self.load_raw()
            if self.gene_centric is not None:
                return self

        self.log("Building Gene from MAF and ASCAT")
        gene_df = df_builders.get_gene_df(maf_df, self.index_name, unique_fields=["gene_id"])
        # ascat_gene_df = df_builders.get_gene_df(
        gistic_gene_df = df_builders.get_gene_df(
            # ascat_df, self.index_name, unique_fields=["gene_id"]
            gistic_df, self.index_name, unique_fields=["gene_id"]
        )
        # gene_df = gene_df.union(ascat_gene_df).distinct()
        gene_df = gene_df.union(gistic_gene_df).distinct()
        self.log_count(gene_df)

        self.log("Building Case subtree")
        # case_subtree = self.build_case_subtree(maf_df, ascat_df, case_df, primary_aliquot_df)
        case_subtree = self.build_case_subtree(maf_df, gistic_df, case_df, primary_aliquot_df)

        self.log('Joining Gene with Case subtree [inner, "gene_id"]')
        gene_centric = gene_df.join(
            case_subtree, gene_df.gene_id == case_subtree.gene_id, "inner"
        ).drop(case_subtree.gene_id)

        self.log_count(gene_centric)

        self.gene_centric = gene_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    def build_case_subtree(
        self,
        maf_df: sql.DataFrame,
        # ascat_df: sql.DataFrame,
        gistic_df: sql.DataFrame,
        case_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
    ) -> sql.DataFrame:
        """
        - build_ssm_subtree
        - build_cnv_subtree
        - join them together
        """
        self.log("Building Case with gene info from MAF and GeneModel")
        case_and_gene_df = self._build_case_with_gene_id(
            maf_df,
            # ascat_df,
            gistic_df,
            case_df,
        )

        self.log("Building SSM subtree")
        ssm_df = self.build_ssm_subtree(maf_df, primary_aliquot_df)
        self.log_count(ssm_df)

        self.log("Building CNV subtree")
        # cnv_df = self.build_cnv_subtree(ascat_df)
        cnv_df = self.build_cnv_subtree(gistic_df)
        self.log_count(cnv_df)

        self.log("Join SSM and CNV subtrees to Case [left, gene_id, case_id]")
        case_subtree = (
            case_and_gene_df.join(ssm_df, on=["gene_id", "case_id"], how="left")
            .join(cnv_df, on=["gene_id", "case_id"], how="left")
            .select(
                "gene_id",
                F.struct("ssm", "cnv", *case_df.drop("gene_id").columns).alias("case"),
            )
        )
        self.log_count(case_subtree)

        self.log('Grouping by case_id and aggregating to list under "gene"')
        case_subtree = case_subtree.groupBy(case_subtree.gene_id.alias("gene_id")).agg(
            F.collect_list("case").alias("case")
        )
        return case_subtree

    def build_ssm_subtree(
        self, maf_df: sql.DataFrame, primary_aliquot_df: sql.DataFrame
    ) -> sql.DataFrame:
        """
        TODO: This branch is same as in case_centric and can be reused
        ssm[]
           |___ consequence[]
           |             |_____ transcript{}
           |                          |_____ annotation{}
           |___ observation[]

        """

        # Consequence
        cons_df = self.consequence_builder.build_for_ssm(
            maf_df,
            self.index_name,
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

        # Aggregating SSM
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
        TODO: This branch is same as in case_centric and can be reused
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

    # def _build_case_with_gene_id(self, maf_df, ascat_df, case_df):
    def _build_case_with_gene_id(self, maf_df, gistic_df, case_df):
        self.log("\nSelecting Gene from MAF")
        # maf_and_ascat_df = (
        maf_and_gistic_df = (
            maf_df.select("case_id", "gene_id")
            # .union(ascat_df.select("case_id", "gene_id"))
            .union(gistic_df.select("case_id", "gene_id"))
            .distinct()
        )

        self.log("Getting gene_id for each case via joining with gene_df")
        # case_gene_id = maf_and_ascat_df.join(case_df, on="case_id").select(
        case_gene_id = maf_and_gistic_df.join(case_df, on="case_id").select(
            "gene_id", *case_df.columns
        )
        return case_gene_id
