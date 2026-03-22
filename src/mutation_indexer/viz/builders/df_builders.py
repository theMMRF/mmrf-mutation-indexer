"""
This module contains logic for building a particular sub-struct within a given index.

NOTE: Please avoid using this pattern. Instead build a single superset of the data using
a `base.Builder` whose singular (cached/backed up) output can be used by multiple
dependent builders. This avoids similar data being built multiple times.
"""

import itertools
import logging
from collections.abc import Container, Iterable, Iterator

import more_itertools
from gdcmodels import mapper
from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import utils

logger = logging.getLogger("df_builder")


def build_ssm_subtree(
    maf_df: sql.DataFrame,
    cons_df: sql.DataFrame,
    index_name: str,
    obs_df: sql.DataFrame | None = None,
) -> sql.DataFrame:
    """
    ssm[]
       |___ consequence[]
       |             |_____...
       |___ observation[]
    """
    ssm_df = get_ssm_df(maf_df, index_name, add_fields=["gene_id", "case_id"])

    df = ssm_df.join(cons_df, on="ssm_id", how="left")
    if obs_df:
        df = df.join(obs_df, on=["ssm_id", "case_id"], how="left")

    return df


def build_cnv_subtree(
    # ascat_df: sql.DataFrame,
    gistic_df: sql.DataFrame,
    index_name: str,
    cons_df: sql.DataFrame | None = None,
    obs_df: sql.DataFrame | None = None,
    add_fields: Iterable[str] = ("gene_id", "case_id"),
) -> sql.DataFrame:
    """
    cnv[]
       |___ consequence[]
       |            |_____ gene{}
       |
       |___ observation[]

    """
    cnv_df = get_cnv_df(
        # ascat_df,
        gistic_df,
        index_name,
        add_fields=add_fields,
        drop_fields=frozenset(("occurrence_id",)),
    )

    df = cnv_df.join(cons_df, on="cnv_id", how="left") if cons_df else cnv_df
    if obs_df:
        df = df.join(obs_df, on=["cnv_id", "case_id"], how="left")

    df = df.drop("occurrence_id")

    return df


def get_annotation_df(
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    return get_single_df(
        input_df,
        index_name,
        "annotation",
        add_fields,
        drop_fields,
        unique_fields,
        ignore,
    )


def get_gene_df(
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore=frozenset(("transcripts",)),
):
    return get_single_df(
        input_df, index_name, "gene", add_fields, drop_fields, unique_fields, ignore
    )


def _get_clinical_annotation_df(
    index_name: str,
    input_df: sql.DataFrame,
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
) -> sql.DataFrame:
    def restructure(doc: dict, parent_name: str = "") -> Iterator[sql.Column]:
        """
        Takes the structure from a mapping and produces arguments for a select
        to reorganize a flat dataframe of clinical annotations into the desired structure.
        Eg:
        Given the mapping:
        ```
        properties:
          clinical_annotations:
            properties:
              civic:
                properties:
                  gene_id:
                    type: keyword
                  variant_id:
                    type: keyword
        ```
        """
        for k, v in doc.items():
            if "properties" in v:
                yield F.struct(*restructure(v["properties"], k)).alias(k)
            elif "type" in v:
                name = v.get("default", f"{parent_name}_{k}")

                yield F.col(name).alias(k)
            else:
                yield F.struct(*restructure(v, k)).alias(k)

    name = "clinical_annotations"
    mapping = utils.select_mapping(index_name, name)
    cols = more_itertools.value_chain("ssm_id", restructure({name: mapping}))
    df = input_df.select(*cols)
    df = df.drop_duplicates(subset=unique_fields)
    cols = (column for column in df.columns if column not in drop_fields)

    return df.select(*cols)


def get_ssm_df(
    maf_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    clinical_anno_df = _get_clinical_annotation_df(index_name, maf_df)
    df = get_single_df(maf_df, index_name, "ssm", add_fields, (), unique_fields, ignore)
    df = df.join(clinical_anno_df, on="ssm_id", how="left")
    columns = (column for column in df.columns if column not in drop_fields)

    return df.select(*columns)


def get_cnv_df(
    # ascat_df: sql.DataFrame,
    gistic_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    return get_single_df(
        # ascat_df, index_name, "cnv", add_fields, drop_fields, unique_fields, ignore
        gistic_df, index_name, "cnv", add_fields, drop_fields, unique_fields, ignore
    )


def get_transcript_df(
    input_df: sql.DataFrame,
    index_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore: Container[str] = (),
) -> sql.DataFrame:
    return get_single_df(
        input_df,
        index_name,
        "transcript",
        add_fields,
        drop_fields,
        unique_fields,
        ignore,
        selector="consequence",
    )


def get_single_df(
    input_df: sql.DataFrame,
    index_name: str,
    mapping_name: str,
    add_fields: Iterable[str] = (),
    drop_fields: Container[str] = (),
    unique_fields: list[str] | None = None,
    ignore: Container[str] = (),
    selector: mapper.Selector | None = None,
) -> sql.DataFrame:
    """Selects the required struct based on the mapping and the data in the data frame.

    Example:
        input_df [{}]
        |---id
        |---center
        +---normal_bam_uuid

        struct:
        mapping:
            properties:
                sub-mapping:
                    center:
                        type: keyword
                    input_bam_file:
                        properties:
                            normal_bam_uuid:
                                type: keyword

        `get_single_df(input_df, "mapping", "sub-mapping", add_fields=("id",))`

        return_df [{}]
        |---id
        |---center
        +---input_bam_file
            +---normal_bam_uuid

    Args:
        input_df: The data frame from which to select the data that is part of the given
            mapping in the index.
        index_name: The index containing the given mapping.
        mapping_name: The name of the mapping that is being built.
        add_fields: An optional sequence of extra fields to include which are not found
            in the mapping. These are usually fields required for joining.
        drop_fields: A set of columns which should not be selected from the input_df but
            are found in the mapping.
        unique_fields: A list of fields with represent a unique row in the data. These
            are used to drop duplicates from the resulting data frame.
        ignore: A set of properties which should not be selected from the mapping.
            These are usually fields which have no corresponding data found with in the
            input_df.
        selector: An optional function to filter the paths found to the given mapping or
            the name of a parent property which must be found in the valid path for the
            mapping. If none, it is assumed there is only one path to the given
            mapping_name in the index mapping.

        Returns:
            A data frame with the restructured data.

    """
    columns = itertools.chain(
        add_fields,
        utils.struct_select(index_name, mapping_name, ignore=ignore, selector=selector),
    )
    df = input_df.select(*columns)
    df = df.drop_duplicates(subset=unique_fields)
    columns = filter(lambda c: c not in drop_fields, df.columns)

    return df.select(*columns)
