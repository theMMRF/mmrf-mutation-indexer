import logging
from collections.abc import Iterable
from typing import TypedDict

from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import lit, broadcast

from mutation_indexer import builders, indexd_utils, schemas
from mutation_indexer.builders import utils
from mutation_indexer.constants import build
from mutation_indexer.viz import configuration

UUIDS_STRUCT = schemas.load_schema("builders/gistic/uuids.yaml")

logger = logging.getLogger(__name__)


def _strip_gene_id() -> sql.Column:
    gene_id = F.col("Locus_ID")

    return F.element_at(F.split(gene_id, r"\."), 1)


@F.udf(returnType=UUIDS_STRUCT)
def _generate_uuids(
    chromosome: str,
    start_position: int,
    end_position: int,
    cnv_change_5_category: int,
    symbol: str,
    gene_id: str,
    is_cancer_gene_census: bool,
    biotype: str,
    case_id: str,
    aliquot_id: str,
) -> dict[str, str]:
    """Creates a uuid struct the following uuids (based on):
        cnv_id (chromosome, start_position, end_position, cnv_change_5_category)
        consequence_id (symbol, gene_id, is_cancer_gene_census, biotype)
        occurrence_id (cnv_id, case_id)
        observation_id (cnv_id, case_id, aliquot_id)

    Returns: UUIDS_STRUCT
    """
    cnv_id = utils.generate_uuid5(
        chromosome, start_position, end_position, cnv_change_5_category
    )

    return {
        "cnv_id": cnv_id,
        "consequence_id": utils.generate_uuid5(
            symbol, gene_id, is_cancer_gene_census, biotype
        ),
        "occurrence_id": utils.generate_uuid5(cnv_id, case_id),
        "observation_id": utils.generate_uuid5(cnv_id, case_id, aliquot_id),
    }


def _add_uuids(gistic_df: sql.DataFrame) -> sql.DataFrame:
    """Adds the following uuids to the dataframe:
        cnv_id
        consequence_id
        occurrence_id
        observation_id
    Which are created using the following columns from the input gistic_df:
        gene_chromosome
        start_position
        end_position
        cnv_change_5_category
        symbol
        gene_id
        is_cancer_gene_census
        biotype
        case_id
        aliquot_id

    Args:
        gistic_df: The GISTIC dataframe with the documented columns present.

    Returns:
        GISTIC data frame with the uuuids added.
    """
    uuids = _generate_uuids(
        "gene_chromosome",
        "start_position",
        "end_position",
        "cnv_change_5_category",
        "symbol",
        "gene_id",
        "is_cancer_gene_census",
        "biotype",
        "case_id",
        "aliquot_id",
    )

    gistic_df = gistic_df.withColumn("uuids", uuids)  # This adds the uuids struct used below.

    return gistic_df.select("*", "uuids.*")


def _add_cnv_change_data(document_df: sql.DataFrame) -> sql.DataFrame:
    """
    Adds the cnv change related values to the data frame.

    Added columns:
        copy_number: This value is the raw copy_number value contained in the file for a
            given gene. It is used to calculate the cnv_change & cnv_change_5_category.

        ploidy_integer: This value is calculated from the lower and upper ploidy value
            of each file. These values are the minimum/maximum modal copy_number values
            respectively within each file. In most cases, this will be a single value
            (2), but in cases where the upper and lower are distinct, the ceiling value
            of the mean is used.

            NOTE: Files with a 0 upper or lower ploidy value are considered contaminated
            data and are removed from indexing. Thus ploidy values are always greater
            than 0.

        cnv_change: This value is based on the copy_number for a gene and its file's
            ploidy values. It is calculated as follows:
                - "Loss": copy_number is less than the lower ploidy.
                - None: copy_number is (inclusively) between the upper and lower ploidy.
                - "Gain": copy_number is greater than the upper ploidy.

        cnv_change_5_category: This value is based on the copy_number for a gene and its
            file's ploidy values. It is calculated as follows:
                - "Homozygous Deletion": copy_number equal to 0
                - "Loss": copy_number is less than the lower ploidy value.
                - None: copy_number is (inclusively) between the upper and lower ploidy.
                - "Gain": copy_number greater than the upper ploidy but less than double
                    the upper ploidy
                - "Amplification": copy_number is greater than or equal to double the
                    upper ploidy.

            NOTE: All cnv_change_5_category values of None, i.e. gene with no change,
            are not indexed and thus removed from the data.

    Args:
        document_df: The data frame containing the copy number data. This must include
            the file_id and copy_number columns

    Returns:
        A copy of the given data frame with the above columns added.
    """
    # document_df = _add_ploidy_values(document_df)
    cnv_change = (
        # F.when(F.col("copy_number") > F.col("upper_ploidy_number"), "Gain")
        # .when(F.col("copy_number") < F.col("lower_ploidy_number"), "Loss")
        F.when(F.col("copy_number") > 0, "Gain")
        .when(F.col("copy_number") < 0, "Loss")
        .otherwise(None)
        .alias("cnv_change")
    )
    cnv_change_5_category = (
        F.when(F.col("copy_number") == 0, "Neutral")
        .when(F.col("copy_number") >= 2, "Amplification")
        .when(F.col("copy_number") <= -2, "Homozygous Deletion")
        .when(F.col("copy_number") == 1, "Gain")
        .when(F.col("copy_number") == -1, "Loss")
        .otherwise(None)
        .alias("cnv_change_5_category")
    )
    # mean_ploidy = (F.col("upper_ploidy_number") + F.col("lower_ploidy_number")) / 2

    document_df = document_df.select(
        "*",
        cnv_change,
        cnv_change_5_category,
        lit(0).alias("sample_ploidy_integer")
        # F.ceil(mean_ploidy).cast("integer").alias("sample_ploidy_integer"),
    ).na.drop(subset="cnv_change_5_category")

    return document_df


class GISTICInputs(TypedDict):
    gistic_metadata_df: sql.DataFrame
    gene_model_df: sql.DataFrame


class GISTICBuilder(builders.InputBuilder[configuration.GISTICBuilder, GISTICInputs]):
    __slots__ = ("_document_dataframe_util",)

    def __init__(
        self,
        config: configuration.GISTICBuilder,
        spark_session: sql.SparkSession,
        document_dataframe_util: indexd_utils.DataFrameUtil,
    ) -> None:
        super().__init__(
            config, spark_session, input_type=GISTICInputs, output=build.DataFrame.GISTIC
        )

        self._document_dataframe_util = document_dataframe_util

    def _build_document_df(self, doc_ids: Iterable[str]) -> sql.DataFrame:
        document_df = self._document_dataframe_util.get_dataframe(
            doc_ids, schema=schemas.load_schema("builders/gistic/gistic_document.yaml")
        )

        document_df = document_df.select(
            *[c for c in document_df.columns if c not in ["Gene_Symbol", "Locus_ID", "Cytoband", "did"]],
            F.col("did").alias("file_id"),
            _strip_gene_id().alias("gene_id"),
        )

        return document_df

    def _build_from_scratch(self, input_dfs: GISTICInputs) -> sql.DataFrame:
        """Builds the GISTIC dataframe

        gistic {}
        |---_id
        |---aliquot_id
        |---biotype
        |---canonical_transcript_id
        |---canonical_transcript_length
        |---canonical_transcript_length_cds
        |---canonical_transcript_length_genomic
        |---case_id
        |---chromosome
        |---cnv_change
        |---cnv_change_5_category
        |---cnv_id
        |---consequence_id
        |---cytoband
        |---description
        |---end_position
        |---entrez_gene
        |---gene_chromosome
        |---gene_end
        |---gene_id
        |---gene_level_cn
        |---gene_start
        |---gene_strand
        |---hgnc
        |---is_cancer_gene_census
        |---name
        |---ncbi_build
        |---observation_id
        |---occurrence_id
        |---omim_gene
        |---start_position
        |---symbol
        |---synonyms
        |---transcripts [{}]
        |   +---(see gene_model.py)
        |---uniprotkb_swissprot
        |---variant_caller
        +---variant_status
        """

        gistic_metadata_df = input_dfs["gistic_metadata_df"]
        gistic_metadata_df = gistic_metadata_df
        gene_model_df = input_dfs["gene_model_df"]

        gene_model_df = (
            gene_model_df.select(
                F.col("_gene_id").alias("gene_id"),
                "_id",
                "biotype",
                "canonical_transcript_id",
                "chromosome",
                "cytoband",
                "description",
                F.col("gene_end").alias("end_position"),
                "entrez_gene",
                F.col("chromosome").alias("gene_chromosome"),
                "gene_end",
                "gene_start",
                "gene_strand",
                "hgnc",
                "is_cancer_gene_census",
                "name",
                "omim_gene",
                F.col("gene_start").alias("start_position"),
                "synonyms",
                "symbol",
                "transcripts",
                "uniprotkb_swissprot",
            )
            .where(utils.is_protein_coding())
            .where(utils.is_between_chr1_and_chr22())
        )
        document_df = self._build_document_df(
            r.file_id for r in gistic_metadata_df.select("file_id").toLocalIterator()
        )

        c_cols = [col for col in document_df.columns if col.startswith("MMRF_")]
        n = len(c_cols)
        # stack_expr = f"stack({n}, " + ", ".join([f"'{c}', `{c}`" for c in c_cols]) + ") as (case_id, copy_number)"
        stack_expr = f"stack({n}, " + ", ".join([f"'{c}', `{c}`" for c in c_cols]) + ") as (sample_submitter_id, copy_number)"
        # document_df = document_df.select("file_id", "gene_id", F.expr(stack_expr))
        document_df = document_df.select("gene_id", F.expr(stack_expr))

        # Joining w/ gene model removes X/Y chromosomes & non-protein coding genes.
        # This should be done before calculating the cnv change value.
        gistic_df = document_df.join(gene_model_df, on="gene_id", how="inner")
        # gistic_df = document_df.join(gene_model_df, document_df["gene_id"] == gene_model_df["gene_id"], how="inner")
        gistic_df = _add_cnv_change_data(gistic_df)
        # gistic_df = gistic_df.join(gistic_metadata_df, on="file_id", how="inner")
        gistic_df = gistic_df.join(broadcast(gistic_metadata_df), gistic_df["sample_submitter_id"] == gistic_metadata_df["submitter_id"], how="inner")
        gistic_df = utils.add_canonical_transcript_lengths(gistic_df)
        gistic_df = _add_uuids(gistic_df)

        return gistic_df.select(
            "_id",
            "aliquot_id",
            "biotype",
            "canonical_transcript_id",
            "canonical_transcript_length",
            "canonical_transcript_length_cds",
            "canonical_transcript_length_genomic",
            "case_id",
            "chromosome",
            "cnv_change",
            "cnv_change_5_category",
            "cnv_id",
            "consequence_id",
            "copy_number",
            "cytoband",
            "description",
            "end_position",
            "entrez_gene",
            "gene_chromosome",
            "gene_end",
            "gene_id",
            F.lit(True).alias("gene_level_cn"),
            "gene_start",
            "gene_strand",
            "hgnc",
            "is_cancer_gene_census",
            "name",
            F.lit("GRCh38").alias("ncbi_build"),
            "observation_id",
            "occurrence_id",
            "omim_gene",
            "sample_ploidy_integer",
            F.col("file_id").alias("src_file_id"),
            "start_position",
            "symbol",
            "synonyms",
            "transcripts",
            "uniprotkb_swissprot",
            F.col("workflow_type").alias("variant_caller"),
            F.lit("Tumor Only").alias("variant_status"),
        )


# class GisticBuilder(BaseInputBuilder):
#     """
#     Read in gistic file and format it for cnv index.

#     NOTE: For CNVs, start and end positions exactly match gene_start and gene_end
#     (c) Kyle Hernandez

#     NOTE: gene_level_cn = True is a placeholder for future use (c) Junjun

#     NOTE: ncbi_build = 'GRCh38' - constant value, same as in ssm branch (c) Zhenyu
#     """

#     def __init__(self, config, sqlContext):
#         super(GisticBuilder, self).__init__(config, sqlContext, 'gistic')

#     def build_from_cache(self, df):
#         return df

#     def build_from_scratch(self, gene_model_df: sql.DataFrame, **kwargs: sql.DataFrame) -> sql.DataFrame:
#         """
#         Read, combine and transform gistic files

#         Args:
#             gene_model_df: The output of the GeneModelBuilder.

#         Returns gistic_df
#         """
#         gistic_df = self.combine()

#         # add gene information
#         gistic_df = self._add_gene_information(gistic_df, gene_model_df)

#         # add canonical_transcript_lengths
#         gistic_df = self.add_canonical_transcript_lengths(gistic_df)

#         # add case_id based on aliquot_id
#         gistic_df = self._add_case_id(gistic_df)

#         # add cnv_id
#         gistic_df = self._add_cnv_id(gistic_df)

#         # add available variation data
#         gistic_df = self._add_available_variation_data(gistic_df)

#         # add consequence_id
#         gistic_df = self._add_consequence_id(gistic_df)

#         # add observation_id
#         gistic_df = self._add_observation_id(gistic_df)

#         # add occurrence_id
#         gistic_df = self._add_occurrence_id(gistic_df)

#         # drop entries with cnv_change == 0 and cast cnv_change to string
#         gistic_df = self._cnv_change_to_string_and_drop_zero(gistic_df)

#         self.logger.info('Caching Gistic dataframe')
#         # NOTE: Do not remove next step. This is a workaround for
#         # "udf requires attributes from more than one child" Spark issue
#         # See https://forums.databricks.com/questions/9401/pyspark-20-withcolumn-using-udf-on-two-columns-and.html
#         gistic_df.cache().count()

#         return gistic_df

#     def combine(self, urls=None):
#         """
#         Combines data frames from a list of urls
#         """
#         if urls is None and self.urls is not None:
#             urls = self.urls
#         elif urls is None and self.urls is None:
#             self.logger.error('Urls not passed and get_urls() not yet called')
#             raise Exception

#         gistic_df = None
#         for url in urls:
#             try:
#                 new_df = self.file_to_df(url)
#                 if self.config.debug:
#                     self.logger.info('Read {} rows from {}'.format(new_df.count(),
#                                                                    url))
#                 # prepare to melt
#                 new_df = self._trim_gene_symbol(new_df)

#                 new_df = remove_columns(new_df, 'Locus ID', 'Cytoband')

#                 # melt dataframe (opposite of pivoting)
#                 # required to get dfs with the same number of columns
#                 # so we can union them together
#                 new_df = melt_df(new_df,
#                                  id_vars=["gene_id"],
#                                  var_name="aliquot_id",
#                                  value_name="cnv_change")

#                 if gistic_df is None:
#                     gistic_df = new_df
#                 else:
#                     gistic_df = gistic_df.union(new_df)
#             except BaseException as e:
#                 self.logger.error(e)

#         return gistic_df

#     def _add_cnv_id(self, gistic_df):
#         """
#         cnv_id ~ (chromosome, gene_start, gene_end, cnv_change)

#         NOTE: start_position and end_position are matching with
#               gene_start and gene_end in gistic context (c) Zhenyu and Kyle
#         """
#         gistic_df = gistic_df.withColumn('cnv_id', uuid5_col(
#             col('chromosome'),
#             col('start_position'),
#             col('end_position'),
#             col('cnv_change')
#         ))
#         return gistic_df

#     def _add_consequence_id(self, gistic_df):
#         """
#         consequence_id ~ (symbol, gene_id, is_cancer_gene_census, biotype)
#         """
#         gistic_df = gistic_df.withColumn('consequence_id', uuid5_col(
#             col('symbol'),
#             col('gene_id'),
#             col('is_cancer_gene_census'),
#             col('biotype')
#         ))
#         return gistic_df

#     def _add_occurrence_id(self, gistic_df):
#         """
#         occurrence_id ~ (cnv_id, case_id)
#         """
#         gistic_df = gistic_df.withColumn('occurrence_id', uuid5_col(
#             col('cnv_id'),
#             col('case_id')
#         ))
#         return gistic_df

#     def _add_observation_id(self, gistic_df):
#         """
#         observation_id ~ (cnv_id, case_id, aliquot_id)
#         """
#         gistic_df = gistic_df.withColumn('observation_id', uuid5_col(
#             col('cnv_id'),
#             col('case_id'),
#             col('aliquot_id')
#         ))
#         return gistic_df

#     def _trim_gene_symbol(self, initial_cnv_df):
#         """
#         Gistic file includes something else
#         We want to trim it.
#         E.g., ENSG00000008128.21 should be ENSG00000008128
#         Unfortunately there is no easy way to do this in place,
#         so we must add the trimmed column and remove the old column.
#         """

#         def trim_gene_symbol_inner(gene_id):
#             period_location = gene_id.rfind('.')
#             if period_location != -1:
#                 gene_id = gene_id[:period_location]

#             return gene_id

#         trim_gene_symbol_udf = udf(trim_gene_symbol_inner, StringType())
#         trimmed_df = initial_cnv_df.withColumn('gene_id',
#                                                trim_gene_symbol_udf(
#                                                    col('Gene Symbol')
#                                                ))

#         trimmed_and_deduped_df = trimmed_df.drop('Gene Symbol')

#         return trimmed_and_deduped_df

#     def _add_gene_information(self, gistic_df: sql.DataFrame, gene_model_df: sql.DataFrame) -> sql.DataFrame:
#         gene_to_cnv_col_names = {'chromosome': 'gene_chromosome',
#                                  'gene_start': 'start_position',
#                                  'gene_end': 'end_position'}

#         # add gene info to gistic_df
#         new_df = gistic_df.join(gene_model_df, gistic_df.gene_id == gene_model_df._gene_id)

#         # Create cnv columns from gene model
#         for old, new in gene_to_cnv_col_names.items():
#             new_df = new_df.withColumn(new, col(old))

#         # extra columns not included in gene model df
#         new_df = self._add_ncbi_build(new_df)
#         new_df = self._add_gene_level_cn(new_df)
#         new_df = self._add_variant_fields(new_df)

#         return new_df

#     def _add_ncbi_build(self, initial_cnv_df):

#         cnv_df_with_ncbi_build = \
#             initial_cnv_df.withColumn('ncbi_build', lit('GRCh38'))

#         return cnv_df_with_ncbi_build

#     def _add_gene_level_cn(self, initial_cnv_df):

#         cnv_df_with_gene_level_cn = \
#             initial_cnv_df.withColumn('gene_level_cn', lit(True))

#         return cnv_df_with_gene_level_cn

#     def _add_variant_fields(self, initial_df):
#         """
#         For now this is a placeholder.
#         """
#         new_df = (
#             initial_df.withColumn('variant_status', lit('Tumor only'))
#                       .withColumn('variant_caller', lit('GISTIC2'))
#         )
#         return new_df

#     def _add_available_variation_data(self, gistic_df):
#         """
#         Populates available_variation_data with 'cnv'.
#         """

#         # Get set of cnv cases from gistic_df
#         gistic_df = (
#             gistic_df.withColumn('available_variation_data',
#                                  lit('cnv')))

#         return gistic_df

#     def _add_case_id(self, df):
#         """
#         Looks up aliquot_id to case_id mapping from gdc_from_graph.case
#         and adds case_id column accordingly
#         """
#         # get list of aliquot_ids to transform
#         aliquot_ids = df.select('aliquot_id').distinct().rdd.map(lambda x: x[0]).collect()

#         # query all case_documents that have relevant aliquots attached
#         query = {
#             "query": {
#                 "constant_score": {
#                     "filter": {
#                         "terms": {
#                             "aliquot_ids": aliquot_ids
#                         }
#                     }
#                 }
#             },
#             '_source': ['aliquot_ids']
#         }

#         relevant_cases = iterate_es_results(
#             self.config.source_es,
#             index_name=self.config.graph_case_index,
#             doc_type=self.config.graph_case_doc_type,
#             query=query,
#         )

#         # build aliquot to case mapping
#         aliquot_to_case_map = {}
#         for case in relevant_cases:
#             for aliquot_id in case['_source']['aliquot_ids']:
#                 aliquot_to_case_map[aliquot_id] = case['_id']

#         # create case_id column based on aliquot_id column, drop aliquot_id
#         def map_aliquot_to_case(aliquot):
#             return aliquot_to_case_map.get(aliquot)

#         df = map_create_column(df, map_aliquot_to_case, 'aliquot_id', 'case_id')

#         return df

#     def _cnv_change_to_string_and_drop_zero(self, df):
#         """
#         We map input cnv_change str number codes to str interpretations
#         (i.e., '2' becomes 'Amplification').
#         We map '0' to None and drop rows that have 'cnv_change' == None.
#         """
#         # rename col to replace
#         df = df.withColumnRenamed('cnv_change', 'cnv_change_init')

#         cnv_change_mapping = {'-1': 'Loss',
#                               '0': None,
#                               '1': 'Gain'}

#         def stringify_cnv_change_inner(int_cnv):
#             try:
#                 return cnv_change_mapping[int_cnv]
#             except KeyError as e:
#                 raise e

#         stringify_cnv_change_udf = udf(stringify_cnv_change_inner,
#                                        StringType())
#         new_df = df.withColumn('cnv_change',
#                                stringify_cnv_change_udf(
#                                    col('cnv_change_init')))

#         new_df = new_df.drop('cnv_change_init')

#         # drop 0/None
#         new_df = new_df.na.drop(subset=['cnv_change'])

#         return new_df
