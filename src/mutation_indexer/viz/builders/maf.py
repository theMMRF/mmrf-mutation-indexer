import logging
from typing import TypedDict, cast

import importlib_resources as resources
import more_itertools
import yaml
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

from mutation_indexer import builders, indexd_utils, pyspark_extensions, schemas
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

logger = logging.getLogger(__name__)


def _ssm_label() -> sql.Column:
    """
    Creates a column with a label (genomic change) from an ssm based on its variant type.
    """
    chromosome = F.regexp_replace("chromosome", "chr", "")
    variant_type = F.col("variant_type")
    start_position = F.col("start_position")
    end_position = F.col("end_position")
    reference_allele = F.col("reference_allele")
    tumor_allele = F.col("tumor_allele")
    multi_nucleotide_polymorphisms = ("DNP", "TNP", "ONP")

    return (
        F.when(
            variant_type == "SNP",
            F.format_string(
                "chr%s:g.%s%s>%s",
                chromosome,
                start_position,
                reference_allele,
                tumor_allele,
            ),
        )
        .when(
            variant_type.isin(*(F.lit(t) for t in multi_nucleotide_polymorphisms)),
            F.format_string(
                "chr%s:g.%s_%sdelins%s",
                chromosome,
                start_position,
                end_position,
                tumor_allele,
            ),
        )
        .when(
            variant_type == "DEL",
            F.format_string("chr%s:g.%sdel%s", chromosome, start_position, reference_allele),
        )
        .when(
            variant_type == "INS",
            F.format_string(
                "chr%s:g.%s_%sins%s",
                chromosome,
                start_position,
                end_position,
                tumor_allele,
            ),
        )
        .otherwise(chromosome)
    )


class MAFInputs(TypedDict):
    maf_metadata_df: sql.DataFrame
    gene_model_df: sql.DataFrame
    civic_dna_df: sql.DataFrame
    civic_protein_df: sql.DataFrame


class MAFBuilder(builders.InputBuilder[configuration.MAFBuilder, MAFInputs]):
    """
    Class responsible for assembling maf files into a single dataframe with
    uniform features
    """

    __slots__ = ("_doc_dataframe_util", "annotation_builders", "schema")

    def __init__(
        self,
        config: configuration.MAFBuilder,
        spark_session: sql.SparkSession,
        doc_dataframe_util: indexd_utils.DataFrameUtil,
    ):
        super().__init__(
            config, spark_session, input_type=MAFInputs, output=build.DataFrame.MAF
        )

        self.schema = self.get_schema()
        self._doc_dataframe_util = doc_dataframe_util

    def _add_civic_annotations(
        self, maf_df: sql.DataFrame, dna_df: sql.DataFrame, protein_df: sql.DataFrame
    ) -> sql.DataFrame:
        df = maf_df.join(
            dna_df,
            on=["chromosome", "start_position", "reference_allele", "tumor_allele"],
            how="left",
        )
        protein_df = protein_df.withColumnRenamed(
            "civic_gene_id", "_civic_gene_id"
        ).withColumnRenamed("civic_variant_id", "_civic_variant_id")
        df = df.join(protein_df, on=["name", "hgvsp_short"], how="left")
        df = df.withColumns(
            {
                "civic_gene_id": F.coalesce("civic_gene_id", "_civic_gene_id"),
                "civic_variant_id": F.coalesce("civic_variant_id", "_civic_variant_id"),
            }
        )

        return df.drop("_civic_gene_id", "_civic_variant_id")

    def _build_from_scratch(self, input_dfs: MAFInputs) -> sql.DataFrame:
        """
        Builds a master MAF dataframe by combining individual MAFs and augmenting them
        with additional features

        Args:
            maf_metadata_df: The output of the MAFMetadataBuilder
            gene_model_df: The output of the GeneModelBuilder.
            civic_dna_df: The output of the civic.DNABuilder.
            civic_protein_df: The output of the civic.ProteinBuilder.

        Return:
            A data frame containing all of the required data related to MAFS

            MAF {}
            +---???
        """
        gene_model_df = input_dfs["gene_model_df"]
        maf_metadata_df = input_dfs["maf_metadata_df"]

        df = self._build_document_dataframe(maf_metadata_df)

        df = self.add_available_variation_data(df)
        # Add label identifying the mutation
        df = self.add_genomic_dna_change(df)
        # Add mutation_type
        df = self.add_mutation_type(df)
        # Add mutation_subtype
        df = self.add_mutation_subtype(df)
        # ssm_id from hashing unique columns in the maf
        df = self.add_ssm_id(df)
        # Create occurrence_id
        df = self.add_occurrence_id(df)
        # Get cds columns from cds_position
        df = self.extract_cds_position(df)
        # Extract sift and polyphen columns
        df = utils.extract_sift_polyphen(df)

        cols_to_drop = frozenset(gene_model_df.columns)
        df = df.select(*[c for c in df.columns if c not in cols_to_drop])
        df = df.join(gene_model_df, df.gene_id == gene_model_df._gene_id, "inner")
        df = df.drop("_gene_id")
        df = self.add_null(df)
        df = utils.add_canonical_transcript_lengths(df)
        df = self.add_normal_genotype(df)
        df = self.map_transform(df)
        df = df.withColumn("variant_process", F.lit("masked"))
        df = self.format_chr(df)
        df = self.format_cosmic_id(df)
        df = df.withColumn(
            "domains",
            F.regexp_replace("domains", r"PDB-ENSP_mappings:\w{4}\.\w;?", ""),
        )
        df = self._add_civic_annotations(
            df, input_dfs["civic_dna_df"], input_dfs["civic_protein_df"]
        )

        logger.info("Repartitioning MAF dataframe")
        df = df.repartition(self._config.repartition_size, "ssm_id")

        return df

    def map_transform(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Transforms maf_df according to maf.yml :type and :pattern
        """
        for column in df.columns:
            if column in self.schema:
                if "type" in self.schema[column]:
                    val_type = self.schema[column]["type"]
                    assert val_type in ["float", "int", "str", "boolean"]
                    df = df.withColumn(column, df[column].cast(val_type))

                elif "pattern" in self.schema[column]:
                    pattern = self.schema[column]["pattern"]

                    def apply_pattern(value):
                        return pattern.format(value)

                    df = df.withColumn(
                        column,
                        F.udf(apply_pattern, types.StringType())(df[column]),
                    )
                else:
                    pass
        return df

    def add_null(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds a null column to use as defaults for mappings.
        """
        return df.withColumn("empty", F.lit(None).cast(types.StringType()))

    def standardize_schema(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Renames and select required columns from the MAF documents
        """
        df_columns = frozenset(df.columns)

        # Map old columns to their new names as given in the schema.
        # Some columns are optional; supply None values for those as specified.
        def standardize(new_column, props):
            old_column = props["name"]
            if old_column in df_columns:
                return F.col(old_column).alias(new_column)
            else:
                raise KeyError(f"Required column {old_column} missing from MAF")

        # Iterate over the output schema rather than the input dataframe.
        # As long as we don't modify the schema after loading it, this should
        # ensure that we output columns in a consistent order.
        return df.select(*[standardize(k, v) for k, v in self.schema.items()])

    def get_schema(self) -> dict[str, dict[str, str]]:
        """
        Load the intended MAF schema from the local YAML file
        """
        resource = resources.files("mutation_indexer.schemas") / "maf.yml"

        return yaml.safe_load(resource.read_bytes())["maf_schema"]

    def format_cosmic_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Turns StringType() cosmic_id field to ArrayType(StringType()) field
        """

        def to_array(cosmic_string):
            if cosmic_string is not None:
                if ";" in cosmic_string:
                    cosmic_string = cosmic_string.split(";")
                else:
                    cosmic_string = [cosmic_string]
            return cosmic_string

        to_array = F.udf(to_array, types.ArrayType(types.StringType()))
        df = df.withColumn("cosmic_id", to_array(df["cosmic_id"]))
        return df

    def add_available_variation_data(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Populates available_variation_data with ['ssm']
        for all cases with mutations
        WARNING: Requires that cases that have been tested in the calling
        pipelines be present in the MAF. If a case was tested but was not
        called, it should have an empty row with only the case_id
        """
        avd_udf = F.udf(
            lambda x, y: [] if (x is None and y is not None) else ["ssm"],
            types.ArrayType(types.StringType()),
        )
        return df.withColumn(
            "available_variation_data",
            avd_udf(F.col("tumor_sample_barcode"), F.col("case_id")),
        )

    def add_mutation_type(self, df: sql.DataFrame) -> sql.DataFrame:
        def mutation_type(mut_type):
            types = {"Somatic": "Simple Somatic Mutation"}
            if mut_type in types:
                return types[mut_type]
            else:
                return None

        mut_type_udf = F.udf(mutation_type, types.StringType())
        df = df.withColumn("mutation_type", mut_type_udf("mutation_type"))
        return df

    def format_chr(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Removes 'chr' from chromosome columns
        chr1 -> 1
        """
        return df.withColumn(
            "gene_chromosome",
            F.udf(lambda x: x.replace("chr", ""), types.StringType())(
                F.col("gene_chromosome")
            ),
        )

    def add_mutation_subtype(self, df: sql.DataFrame) -> sql.DataFrame:
        def subtype(variant_type):
            subtypes = {
                "SNP": "Single base substitution",
                "DEL": "Small deletion",
                "INS": "Small insertion",
                "DNP": "Di-nucleotide polymorphism",
                "TNP": "Tri-nucleotide polymorphism",
                "ONP": "Oligo-nucleotide polymorphism",
            }
            if variant_type in subtypes:
                return subtypes[variant_type]
            else:
                return None

        sub_type_udf = F.udf(subtype, types.StringType())
        df = df.withColumn("mutation_subtype", sub_type_udf("variant_type"))

        return df

    def add_normal_genotype(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds normal_genotype column to the MAF dataframe
        """
        maf_df = df.withColumn(
            "normal_genotype",
            F.struct(
                utils.uuid5_col(
                    F.col("match_norm_seq_allele1"),
                    F.col("match_norm_seq_allele2"),
                ).alias("allele_id")
            ),
        )
        return maf_df

    def add_ssm_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds ssm_id column to the MAF dataframe
        """
        maf_df = df.withColumn(
            "ssm_id",
            utils.uuid5_col(
                F.lit("ssm"),
                F.col("ncbi_build"),
                F.col("chromosome"),
                F.col("start_position"),
                F.col("end_position"),
                F.col("mutation_subtype"),
                F.col("reference_allele"),
                F.col("tumor_allele"),
            ),
        )
        return maf_df

    def add_occurrence_id(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds the occurrence_id, a uuid hash of:
        'ssm_occurrence' + ssm_id + case_id
        """
        df = df.withColumn(
            "occurrence_id",
            utils.uuid5_col(
                F.lit("ssm_occurrence"),
                F.col("ssm_id"),
                F.col("case_id"),
            ),
        )
        return df

    def add_genomic_dna_change(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Adds the genomic_dna_change column
        """
        maf_df = df.withColumn("genomic_dna_change", _ssm_label())
        return maf_df

    def extract_cds_position(self, df: sql.DataFrame) -> sql.DataFrame:
        """
        Extracts cds_start, cds_length and cds_end from the cds_position column
        cds_position: 1273/2112 -> cds_start: 1273,
                                   cds_length: 2112,
                                   cds_end: 1273 + 2112
        cds_position: 1273-1274/2112 -> cds_start: 1273,
                                        cds_length: 2112,
                                        cds_end: 1273 + 2112
        """

        def start(s):
            s = (s and s.split("/")[0].split("-")[0].strip()) or -1
            if s in [-1, "?"]:
                return -1
            return int(s.split("/")[0].split("-")[0])

        def length(s):
            if not (s and s.split("/")[1].strip()):
                return -1
            return int(s.split("/")[1])

        def end(s):
            if -1 in [start(s), length(s)]:
                return -1
            return start(s) + length(s)

        df = df.withColumn(
            "cds_start",
            F.udf(start, types.IntegerType())(F.col("cds_position")),
        )
        df = df.withColumn(
            "cds_end",
            F.udf(end, types.IntegerType())(F.col("cds_position")),
        )
        df = df.withColumn(
            "cds_length",
            F.udf(length, types.IntegerType())(F.col("cds_position")),
        )
        return df

    def _build_document_dataframe(self, maf_metadata_df: sql.DataFrame) -> sql.DataFrame:
        """
        Builds a data frame from the data contained in the files whose ids are
        in the maf_metadata_df

        Args:
            maf_metadata_df: a data frame containing all file ids related to MAFs
                which need to be loaded

        Return:
            A data frame containing all data within the required MAF files.
        """
        files = more_itertools.map_reduce(
            maf_metadata_df.select("file_id", "data_type").distinct().toLocalIterator(),
            keyfunc=lambda row: cast(str, row.data_type),
            valuefunc=lambda row: cast(str, row.file_id),
        )

        masked_somatic_mutaion = files.get("Masked Somatic Mutation", ())
        aggregated_somatic_mutation = files.get("Aggregated Somatic Mutation", ())

        masked_somatic_mutation_df = self._doc_dataframe_util.get_dataframe(
            masked_somatic_mutaion,
            schema=schemas.load_schema("builders/maf/masked_somatic_mutation.yaml"),
            comment="#",
            enforce_schema=False,
        ).drop(
            #"STRAND_VEP",
            #"AF",
            #"AFR_AF",
            #"AMR_AF",
            #"ASN_AF",
            #"EAS_AF",
            #"EUR_AF",
            #"SAS_AF",
            #"AA_AF",
            #"EA_AF",
            #"MINIMISED",
            #"FILTER",
            #"APPRIS",
            #"CC",
            #"CLINVAR",
            #"FLAGS",
            #"flanking_bps",
            #"GNOMAD_EXOME",
            #"GNOMAD_EXOME_AF",
            #"GNOMAD_GENOME",
            #"GNOMAD_GENOME_AF",
            #"gnomADe_AF",
            #"gnomADe_AFR_AF",
            #"gnomADe_AMR_AF",
            #"gnomADe_ASJ_AF",
            #"gnomADe_EAS_AF",
            #"gnomADe_FIN_AF",
            #"gnomADe_NFE_AF",
            #"gnomADe_OTH_AF",
            #"gnomADe_SAS_AF",
            #"MANE",
            #"One_Consequence",
            #"TOPMED",
            #"TOPMED_AF",
            #"TRANSCRIPT_STRAND",
            #"UNIPROT_ISOFORM",
            #"vcf_id",
            #"vcf_pos",
            #"vcf_qual",
            #"COSMIC_NC",
            "FILTER",
            "AA_AF",
            "AF",
            "AFR_AF",
            "AMR_AF",
            "ASN_AF",
            "CC",
            "CLINVAR",
            "COSMIC_NC",
            "EA_AF",
            "EAS_AF",
            "EUR_AF",
            "flanking_bps",
            "GNOMAD_EXOME",
            "GNOMAD_EXOME_AF",
            "GNOMAD_GENOME",
            "GNOMAD_GENOME_AF",
            "gnomADe_AF",
            "gnomADe_AFR_AF",
            "gnomADe_AMR_AF",
            "gnomADe_ASJ_AF",
            "gnomADe_EAS_AF",
            "gnomADe_FIN_AF",
            "gnomADe_NFE_AF",
            "gnomADe_OTH_AF",
            "gnomADe_SAS_AF",
            "SAS_AF",
            "STRAND_VEP",
            "TOPMED",
            "TOPMED_AF",
            "vcf_id",
            "vcf_pos",
            "vcf_qual",
        ).withColumnRenamed("case uuid", "case_id")
        aggregated_somatic_mutation_df = pyspark_extensions.default_columns(
            self._doc_dataframe_util.get_dataframe(
                aggregated_somatic_mutation,
                schema=schemas.load_schema("builders/maf/aggregated_somatic_mutation.yaml"),
                comment="#",
                enforce_schema=False,
            ),
            (
                pyspark_extensions.DefaultColumn(name="normal_bam_uuid"),
                pyspark_extensions.DefaultColumn(name="tumor_bam_uuid"),
                pyspark_extensions.DefaultColumn(name="RNA_alt_count"),
                pyspark_extensions.DefaultColumn(name="RNA_depth"),
                pyspark_extensions.DefaultColumn(name="RNA_ref_count"),
                pyspark_extensions.DefaultColumn(name="RNA_Support"),
                pyspark_extensions.DefaultColumn(
                    name="callers", value="FM Simple Somatic Mutation"
                ),
            ),
        ).drop(
            #"FMI_TRANSCRIPT",
            #"FMI_GENE",
            #"src_vcf_id",
            #"FMI_FUNCTIONAL_EFFECT",
            ##"ALLELE_NUM",
            #"STRAND_VEP",
            #"AF",
            #"AFR_AF",
            #"AMR_AF",
            #"ASN_AF",
            #"EAS_AF",
            #"EUR_AF",
            #"SAS_AF",
            #"AA_AF",
            #"EA_AF",
            #"COSMIC_NC",
            #"Disease_type",
            #"MINIMISED",
            #"FMI_STATUS",
            #"FILTER",
            #"APPRIS",
            #"CC",
            #"CLINVAR",
            #"COSMIC_NC",
            #"FLAGS",
            #"flanking_bps",
            #"GNOMAD_EXOME",
            #"GNOMAD_EXOME_AF",
            #"GNOMAD_GENOME",
            #"GNOMAD_GENOME_AF",
            #"gnomADe_AF",
            #"gnomADe_AFR_AF",
            #"gnomADe_AMR_AF",
            #"gnomADe_ASJ_AF",
            #"gnomADe_EAS_AF",
            #"gnomADe_FIN_AF",
            #"gnomADe_NFE_AF",
            #"gnomADe_OTH_AF",
            #"gnomADe_SAS_AF",
            #"MANE",
            #"One_Consequence",
            #"TOPMED",
            #"TOPMED_AF",
            #"TRANSCRIPT_STRAND",
            #"UNIPROT_ISOFORM",
            #"vcf_id",
            #"vcf_pos",
            #"vcf_qual",
            "FILTER",

            #"callers",
            #"normal_bam_uuid",
            #"tumor_bam_uuid",
            #"RNA_alt_count",
            #"RNA_depth",
            #"RNA_ref_count",
            #"RNA_Support",

            # "ALLELE_NUM",
            # "Disease_type",
            # "MINIMISED",
            
            "1000G_AF",
            "1000G_AFR_AF",
            "1000G_AMR_AF",
            "1000G_EAS_AF",
            "1000G_EUR_AF",
            "1000G_SAS_AF",
            "APPRIS",
            "CONTEXT",
            "Disease_type",
            #"ESP_AA_AF",
            "ESP_EA_AF",
            "FLAGS",
            "FMI_FUNCTIONAL_EFFECT",
            
            "FMI_GENE",
            "FMI_STATUS",
            "FMI_TRANSCRIPT",
            "GDC_FILTER",
            "gnomAD_AF",
            "gnomAD_AFR_AF",
            "gnomAD_AMR_AF",
            "gnomAD_ASJ_AF",
            "gnomAD_EAS_AF",
            "gnomAD_FIN_AF",
            "gnomAD_NFE_AF",
            "gnomAD_non_cancer_AF",
            "gnomAD_non_cancer_AFR_AF",
            "gnomAD_non_cancer_AMI_AF",
            "gnomAD_non_cancer_AMR_AF",
            "gnomAD_non_cancer_ASJ_AF",
            "gnomAD_non_cancer_EAS_AF",
            "gnomAD_non_cancer_FIN_AF",
            "gnomAD_non_cancer_MAX_AF_adj",
            "gnomAD_non_cancer_MAX_AF_POPS_adj",
            "gnomAD_non_cancer_MID_AF",
            "gnomAD_non_cancer_NFE_AF",
            "gnomAD_non_cancer_OTH_AF",
            "gnomAD_non_cancer_SAS_AF",
            "gnomAD_OTH_AF",
            "gnomAD_SAS_AF",
            "hotspot",
            "MANE",
            "MAX_AF",
            "MAX_AF_POPS",
            "miRNA",
            "One_Consequence",
            "src_vcf_id",
            "TRANSCRIPT_STRAND",
            "TRANSCRIPTION_FACTORS",
            "UNIPROT_ISOFORM",
        )

        maf_df = masked_somatic_mutation_df.unionByName(aggregated_somatic_mutation_df)

        return self.standardize_schema(maf_df)
