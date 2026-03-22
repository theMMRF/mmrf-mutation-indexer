from typing import Any

from pyspark import sql
from pyspark.sql import functions as F

from mutation_indexer.builders import utils
from mutation_indexer.viz.builders import df_builders

ALL_EFFECTS_KEYS = (
    "do_not_use",
    "consequence_type",
    "aa_change",
    "transcript_id",
    "ref_seq_accession",
    "hgvsc",
    "vep_impact",
    "is_canonical",
    "sift",
    "polyphen",
    "transcript_strand",
)
NULL_NON_SELECTED_FIELDS = (
    "amino_acids",
    "cdna_position",
    "cds_end",
    "cds_length",
    "cds_position",
    "cds_start",
    "clin_sig",
    "codons",
    "domains",
    "ensp",
    "hgvsp",
    "hgvsp_short",
    "protein_position",
    "swissprot",
    "trembl",
    "uniparc",
)


def _extract_transactions(maf_df: sql.DataFrame) -> sql.DataFrame:
    all_effects = F.col("all_effects")
    ssm_transaction_df = maf_df.withColumn("selected_transcript_id", F.col("transcript_id"))
    # Explode all_effects, to have each individual transcript data on a separate line
    # NOTE: after exploding, missing fields for secondary transcripts will be populated
    # with values from selected transcript (top level columns)
    ssm_transaction_df = ssm_transaction_df.withColumn(
        "all_effects", F.explode(F.split(all_effects, ";"))
    )
    ssm_transaction_df = ssm_transaction_df.withColumn(
        "all_effects",
        F.when(all_effects.contains(","), F.split(all_effects, ",")).otherwise(
            F.split(all_effects, ":")
        ),
    )

    return ssm_transaction_df.withColumns(
        {key: all_effects.getItem(index) for index, key in enumerate(ALL_EFFECTS_KEYS)}
    )


class ConsequenceBuilder:
    """
    Build transcripts for each ssm by joining in data from the gene model
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Dummy init. Class has no dependencies."""
        pass

    def build_for_ssm(
        self,
        maf_df: sql.DataFrame,
        index_name: str,
        join_gene: bool = False,
        add_gene_aa_change: bool = False,
    ) -> sql.DataFrame:
        """
        Extracts transcript_ids from the all_effects maf column for each ssm,
        then joins transcript data from the gene model.
        Returns arrays of transcripts keyed on ssm_id

        Args:
            maf_df: The formatted MAF dataframe from MAFBuilder
            index_name: name of the index this consequence is a part of
            join_gene: Whether or not to join the gene data to the consequence. SSM and
                SSM Occurrence have gene under consequences, while Case and Gene do
                not. Must be true if add_gene_aa_change is true
            add_gene_aa_change: Adds the gene_aa_change field to the data if set to
                True. Can only be set to True if join_gene is set to true also.

        Returns:
            An dataframe containing and an array of transcripts

            consequence
            |---ssm_id
            |---consequence [{}]
            |   |---consequence_id
            |   +---transcript {}
            |       |---transcript_id
            |       |---aa_change
            |       |---consequence_type
            |       |---ref_seq_accession
            |       |---annotation {}
            |       |   |---amino_acids
            |       |   |---ccds
            |       |   |---cdna_position
            |       |   |---cds_end
            |       |   |---cds_length
            |       |   |---cds_position
            |       |   |---cds_start
            |       |   |---clin_sig
            |       |   |---codons
            |       |   |---dbsnp_rs
            |       |   |---dbsp_val_status
            |       |   |---domains
            |       |   |---ensp
            |       |   |---existing_variation
            |       |   |---hgvsc
            |       |   |---hgvsp
            |       |   |---hgvsp_short
            |       |   |---polyphen_impact
            |       |   |---polyphen_score
            |       |   |---protein_position
            |       |   |---pubmed
            |       |   |---sift_impact
            |       |   |---sift_score
            |       |   |---switprot
            |       |   |---transcript_id
            |       |   |---trembl
            |       |   |---uniparc
            |       |   +---vep_impact
            |       +---gene {} (only if join_gene is True)
            |           |---biotype
            |           |---canonical_transcript_id
            |           |---cytoband []
            |           |---external_db_ids {}
            |           |   |---entrez_gene []
            |           |   |---hgnc []
            |           |   |---omim_gene []
            |           |   +---uniprotkb_swissprot []
            |           |---gene_chromosome
            |           |---gene_end
            |           |---gene_id
            |           |---gene_start
            |           |---gene_strand
            |           |---is_cancer_gene_census
            |           |---symbol
            |           +---synonyms []
            +---gene_aa_change [] (only if join_gene and add_gene_aa_change are True)
        """
        if add_gene_aa_change and not join_gene:
            raise ValueError("If add_gene_aa_change is true so must join_gene.")

        # => {gene_id, ssm_id, transcript_id,
        # empty, canonical_tracript_id, is_canonical,
        # do_not_us, consequence_type, aa_change
        # refs_seq_accession}
        ssm_tran = self.build_all_effects_cols(maf_df)

        ann_df = df_builders.get_annotation_df(
            ssm_tran,
            index_name,
            add_fields=["ssm_id"],
            unique_fields=["ssm_id", "transcript_id"],
        )
        ann_df = ann_df.select(
            "ssm_id",
            "transcript_id",
            F.struct(ann_df.drop("ssm_id").columns).alias("annotation"),
        )
        # => {gene_id, ssm_id, transcrpt_id,
        # is_canonical,
        # do_not_us, consequence_type, aa_change,
        # refs_seq_accession}
        tran_df = df_builders.get_transcript_df(
            ssm_tran, index_name, add_fields=["gene_id", "ssm_id"]
        )

        # {*fields} => {*fields, annotation: {}}
        tran_with_ann = tran_df.join(ann_df, on=["ssm_id", "transcript_id"], how="left")

        if join_gene:
            # Build and join the gene if required
            gene_df = self._build_gene_struct(maf_df, index_name)

            # => {ssm_id, transcript_id, *transcript_fields, gene:{}}
            tran_with_ann = tran_with_ann.join(gene_df, on="gene_id")

        # => {ssm_id, consequence {transcript:
        #       {transcript_id, *transcript_fields}}}
        tran_with_ann = tran_with_ann.drop("gene_id", "empty")

        # Add consequence_id, a uuid from ssm_id and transcript_id
        tran_df = tran_with_ann.withColumn(
            "consequence_id",
            utils.uuid5_col(
                F.lit("ssm_consequence"),
                F.col("ssm_id"),
                F.col("transcript_id"),
            ),
        )
        if add_gene_aa_change:
            tran_df = tran_df.withColumn(
                "gene_aa_change",
                F.when(
                    F.col("gene.symbol").isNull() | F.col("aa_change").isNull(),
                    None,
                ).otherwise(F.concat_ws(" ", tran_df.gene.symbol, tran_df.aa_change)),
            )
            tran_df = tran_df.select(
                "ssm_id",
                F.struct(
                    "consequence_id",
                    F.struct(
                        *tran_df.drop("ssm_id", "consequence_id", "gene_aa_change")
                    ).alias("transcript"),
                ).alias("consequence"),
                "gene_aa_change",
            )

            df = tran_df.groupby("ssm_id").agg(
                F.collect_list("consequence").alias("consequence"),
                F.collect_list("gene_aa_change").alias("gene_aa_change"),
            )
            df = utils.sanitize_gene_aa_change(df)

        else:
            tran_df = tran_df.select(
                "ssm_id",
                F.struct(
                    "consequence_id",
                    F.struct(*tran_df.drop("ssm_id", "consequence_id")).alias("transcript"),
                ).alias("consequence"),
            )

            df = tran_df.groupby("ssm_id").agg(
                F.collect_list("consequence").alias("consequence")
            )

        return df

    # def build_for_cnv(self, ascat_df: sql.DataFrame, index_name: str) -> sql.DataFrame:
    def build_for_cnv(self, gistic_df: sql.DataFrame, index_name: str) -> sql.DataFrame:
        """
        For now this is just gene information:

        consequence[]
                |_____ gene{}
        """

        # Create gene structure
        cons_df = (
            # ascat_df.select(
            gistic_df.select(
                "cnv_id",
                F.struct(*utils.struct_select(index_name, "consequence")).alias("consequence"),
            )
            .groupby("cnv_id")
            .agg(F.collect_set("consequence").alias("consequence"))
        )

        return cons_df

    @staticmethod
    def build_all_effects_cols(maf_df: sql.DataFrame) -> sql.DataFrame:
        """
        Extracts information about transcripts from the all_effects column

        all_effects is formated as such:

        BEFORE:
        do_not_use,consequence_type,aa_change,transcript_id,refs_seq_accession;
        MORN1,synonymous_variant,p.=,ENST00000378531,NM_024848.1;

        NEW all_effects fields: (appended after old ones)
        HGVSc,IMPACT,CANONICAL,SIFT,PolyPhen,Transcript_Strand
        c.3602T>G,MODERATE,YES,tolerated(0.06),possibly_damaging(0.614),1

        We need to first extract each row within this column and explode it into
        a new row in the dataframe. We then extract each column from that row
        using the all_effects_udf

        There are some mutations that have transcripts not belonging to the
        gene of that mutation. They can be identified by matching the
        symbol from the mutation to the do_not_use column.
        These should be removed.
        """
        # Before exploding, let's save the transcript_id of the selected transcript
        ssm_transaction_df = _extract_transactions(maf_df)

        # Clear the fields that we shouldn't copy from selected transcript (top level of maf_df)
        for field in NULL_NON_SELECTED_FIELDS:
            ssm_transaction_df = ssm_transaction_df.withColumn(
                field,
                F.when(
                    F.col("transcript_id") == F.col("selected_transcript_id"),
                    F.col(field),
                ).otherwise(None),
            )

        # Take out the transcripts from genes that this mutation is not in
        ssm_transaction_df = ssm_transaction_df.where(F.col("symbol") == F.col("do_not_use"))

        # get is_canonical
        ssm_transaction_df = ssm_transaction_df.withColumn(
            "is_canonical",
            F.col("canonical_transcript_id") == F.col("transcript_id"),
        )

        ssm_transaction_df = utils.sanitize_aa_change(ssm_transaction_df)
        # Get aas columns from aa_change
        ssm_transaction_df = utils.extract_aas_position(ssm_transaction_df)
        ssm_transaction_df = utils.convert_empty_str_to_null_in_col(
            ssm_transaction_df, "aa_change"
        )

        # Extract sift, polyphen columns
        ssm_transaction_df = utils.extract_sift_polyphen(ssm_transaction_df)

        # Drop used helper columns
        ssm_transaction_df = ssm_transaction_df.drop("all_effects", "do_not_use", "symbol")

        return ssm_transaction_df

    def _build_gene_struct(self, maf_df: sql.DataFrame, index_name: str) -> sql.DataFrame:
        # Build and join the gene if required

        to_drop = [
            "transcripts",
            "description",
            "canonical_transcript_length",
            "name",
            "canonical_transcript_length_cds",
            "canonical_transcript_length_genomic",
        ]

        gene_df = df_builders.get_gene_df(maf_df, index_name, drop_fields=to_drop)
        gene_struct_df = gene_df.select("gene_id", F.struct("*").alias("gene"))
        return gene_struct_df
