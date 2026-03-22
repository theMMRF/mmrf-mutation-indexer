import logging

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import utils
from mutation_indexer.constants import app

logging.basicConfig(format=app.LOG_FORMAT)


class ObservationBuilder:
    """
    Builds observation dataframe from the maf dataframe
    """

    def build_for_ssm(
        self,
        maf_df: sql.DataFrame,
        primary_aliquot_df: sql.DataFrame,
        index_name: str,
        selector: str | None = None,
    ) -> sql.DataFrame:
        """
        Builds an observation from a maf.
        Each line of a maf is roughly an observation, though it could be better
        said that a unique observation is identified by a unqiue pairing of
        tumor and normal sample uuids and an ssm uuid.
        """
        # Select all of the nested fields
        primary_aliquot_df = primary_aliquot_df.where(F.col("entity") == F.lit("case"))
        flat_obs_df = maf_df.select(
            "ssm_id",
            "case_id",
            "occurrence_id",
            *utils.select_nested(
                index_name, "observation", selector=selector, ignore=["observation_id"]
            ),
        ).join(primary_aliquot_df, ["case_id"], how="left")

        # unique_values_variant_caller = flat_obs_df.select("variant_caller").distinct()

        # flat_obs_df = (
        #     flat_obs_df.withColumn("variant_caller", F.explode(F.split("variant_caller", ";")))
        #     .withColumn("variant_caller", F.regexp_replace("variant_caller", r"^\*+|\*+$", ""))
        #     .where(F.col("variant_caller") != F.lit("somaticsniper"))
        # )

        flat_obs_df = flat_obs_df.withColumn(
            "observation_id",
            utils.uuid5_col(
                F.lit("ssm_observation"),
                F.col("occurrence_id"),
                F.col("tumor_sample_uuid"),
                F.col("matched_norm_sample_uuid"),
                F.col("variant_caller"),
                F.lit("masked"),
            ),
        )

        return (
            flat_obs_df.select(
                "ssm_id",
                "case_id",
                "occurrence_id",
                F.struct(
                    *utils.struct_select(index_name, "observation", selector=selector)
                ).alias("observation"),
            )
            .groupby("ssm_id", "case_id", "occurrence_id")
            .agg(F.collect_list("observation").alias("observation"))
        )

    def build_for_cnv(
        # self, ascat_df: sql.DataFrame, index: str, selector: str | None = None
        self, gistic_df: sql.DataFrame, index: str, selector: str | None = None
    ) -> sql.DataFrame:
        """
        observation[]
        |____ observation{}
                |____ observation_id
                |____ variant_status
                |____ variant_calling {}
                        |____ variant_caller

        """

        # add other observation fields
        # obs_df = ascat_df.withColumn(
        obs_df = gistic_df.withColumn(
            "variant_calling",
            F.struct("variant_caller").alias("variant_calling"),
        )

        # observation structure
        obs_df = (
            obs_df.select(
                "cnv_id",
                "case_id",
                "occurrence_id",
                F.struct(*utils.struct_select(index, "observation", selector=selector)).alias(
                    "observation"
                ),
            )
            .groupby("cnv_id", "case_id", "occurrence_id")
            .agg(F.collect_set("observation").alias("observation"))
        )

        return obs_df


def build_observation_for_segment_cnv(segment_cnv_df: sql.DataFrame) -> sql.DataFrame:
    """Builds the observation dataframe from the segment_cnv dataframe.

    observation[]
    |____ observation{}
            |____ observation_id
            |____ copy_number
            |____ sample_ploidy_integer
            |____ src_file_id
            |____ variant_status
            |____ variant_calling {}
                    |____ variant_caller
    """
    obs_cols = (
        "observation_id",
        "copy_number",
        "sample_ploidy_integer",
        "src_file_id",
        "variant_status",
        F.struct("variant_caller").alias("variant_calling"),
    )
    obs_df = (
        segment_cnv_df.select(
            "segment_cnv_id",
            "case_id",
            "occurrence_id",
            F.struct(*obs_cols).alias("observation"),
        )
        .groupby("segment_cnv_id", "case_id", "occurrence_id")
        .agg(F.collect_set("observation").alias("observation"))
    )

    return obs_df
