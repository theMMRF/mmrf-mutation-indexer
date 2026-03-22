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


class CNVOccurrenceCentricBuilder(base_builder.BaseBuilder):
    """
    Builds cnv-occurrence-centric dataframe given
    case, gene, and maf dataframes:

    cnv_occurrence{}
        |
        |____ case{}
        |       |____ observation[]
        |
        |____ cnv{}
                |____ consequence[]
                            |_____ gene{}
    """

    index_name = "cnv_occurrence_centric"
    id_field = "cnv_occurrence_id"

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
        # self, ascat_df: sql.DataFrame, case_df: sql.DataFrame, **kwargs: sql.DataFrame
        self, gistic_df: sql.DataFrame, case_df: sql.DataFrame, **kwargs: sql.DataFrame
    ) -> Self:
        """
        Builds CNV Occurrence Centric index
        """
        # Check if we should load a pre-built dataframe
        if self.config.output_raw == "read":
            self.cnv_occurrence_centric = self.load_raw()
            if self.cnv_occurrence_centric is not None:
                return self

        # self.log_count(ascat_df)

        # CNV subtree
        # cnv_df = self.build_cnv_subtree(ascat_df)
        cnv_df = self.build_cnv_subtree(gistic_df)

        # Case subtree
        # case_subtree = self.build_case_subtree(ascat_df, case_df)
        case_subtree = self.build_case_subtree(gistic_df, case_df)

        self.log("Joining cnv with case")

        cnv_occurrence_centric = (
            cnv_df.join(case_subtree, on=["case_id", "cnv_id"], how="inner")
            .withColumnRenamed("occurrence_id", "cnv_occurrence_id")
            .drop("case_id")
            .drop("cnv_id")
        )

        self.log_count(cnv_occurrence_centric)

        self.cnv_occurrence_centric = cnv_occurrence_centric
        self.log("Build finished")

        # Save the resulting dataframe to s3
        self.write()

        return self

    # def build_cnv_subtree(self, ascat_df):
    def build_cnv_subtree(self, gistic_df):
        """
        cnv{}
            |____ consequence[]
                        |_____ gene{}
        """

        # Consequence
        # cons_df = self.consequence_builder.build_for_cnv(ascat_df, self.index_name)
        cons_df = self.consequence_builder.build_for_cnv(gistic_df, self.index_name)

        cnv_df = df_builders.build_cnv_subtree(
            # ascat_df, self.index_name, cons_df=cons_df, add_fields=["case_id"]
            gistic_df, self.index_name, cons_df=cons_df, add_fields=["case_id"]
        )

        cnv_subtree = cnv_df.select(
            "cnv_id",
            "case_id",
            F.struct("consequence", *cnv_df.drop("consequence").drop("case_id").columns).alias(
                "cnv"
            ),
        )

        return cnv_subtree

    # def build_case_subtree(self, ascat_df, case_df):
    def build_case_subtree(self, gistic_df, case_df):
        """
        case{}
            |____ observation[]
        """
        self.log("Building case subtree")

        # Observation
        # obs_df = self.observation_builder.build_for_cnv(ascat_df, self.index_name)
        obs_df = self.observation_builder.build_for_cnv(gistic_df, self.index_name)

        self.log("Join observation with case")
        case_obs_df = case_df.join(obs_df, on="case_id", how="left").select(
            "case_id",
            "occurrence_id",
            "cnv_id",
            F.struct("observation", *case_df.columns).alias("case"),
        )
        self.log_count(case_obs_df)
        return case_obs_df
