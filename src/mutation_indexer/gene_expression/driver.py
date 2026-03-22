"""This module is for implementing how the gene_expression driver will be executed."""

import contextlib
import logging
from collections.abc import Container, Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

import boto3
import mypy_boto3_s3 as s3
from pyspark import sql

from mutation_indexer import driver, es_utils, indexd_utils
from mutation_indexer.configuration import aws
from mutation_indexer.constants import build
from mutation_indexer.databases import sqlite
from mutation_indexer.gene_expression import builders, configuration


def _initialize_s3_client(config: aws.S3) -> s3.Client:
    return boto3.client(
        "s3",
        endpoint_url=config.host,
        # aws_access_key_id=config.access_key,
        # aws_secret_access_key=config.secret_key,
        verify=False,
    )


class Dependencies(NamedTuple):
    doc_dataframe_util: indexd_utils.DataFrameUtil
    es_dataframe_util: es_utils.DataFrameUtil
    mappings_loader: es_utils.MappingsLoader
    s3_client: s3.Client
    spark_session: sql.SparkSession
    sqlite_db: sqlite.SQLiteDatabase


class Driver(driver.Driver[configuration.Configuration]):
    @classmethod
    def load_config(cls, file: Path) -> configuration.Configuration:
        return configuration.Configuration.load(file)

    @contextlib.contextmanager
    def _initialize_dependencies(
        self, config: configuration.Configuration, spark_session: sql.SparkSession
    ) -> Iterator[Dependencies]:
        """Initializes all dependencies upon which the builders depend.

        Args:
            config: The configuration for this run of the driver.
            spark_session: The spark session for this run of the driver.

        Returns:
            A context manager wrapping the dependencies. The context should be exited
            only after the dependent builders and done being used.
        """
        s3_client = _initialize_s3_client(config.aws.s3)

        with (
            driver.get_es_client(config.elasticsearch.connection) as es_client,
            sqlite.SQLiteDatabase(config.sqlite_database, s3_client) as sqlite_db,
        ):
            index_client = driver.get_index_client(config.indexd)
            mappings_loader = es_utils.MappingsLoader()

            yield Dependencies(
                indexd_utils.DataFrameUtil(
                    index_client,
                    sql.SQLContext(spark_session.sparkContext, spark_session),
                    logging.getLogger(indexd_utils.__name__),
                ),
                es_utils.DataFrameUtil(
                    config.elasticsearch,
                    spark_session,
                    es_client,
                    mappings_loader,
                    es_utils.SchemaLoader(),
                ),
                mappings_loader,
                s3_client,
                spark_session,
                sqlite_db,
            )

    def _builders(
        self,
        config: configuration.Builders,
        index_types: Container[build.IndexType],
        dependencies: Dependencies,
    ) -> Iterator[builders.Builder]:
        """Gets all builders associated with the GE driver & used by other builders.

        Args:
            config: The configuration for builders in this run of the driver.
            dependencies: The dependencies for the builders.

        Return:
            An iterable of all gene expression builders.
        """
        yield builders.BinaryBuilder(
            config.binary, dependencies.spark_session, dependencies.s3_client
        )
        yield builders.CaseBuilder(
            config.case, dependencies.spark_session, dependencies.s3_client
        )
        yield builders.CaseSQLBuilder(
            config.case_sql, dependencies.spark_session, dependencies.sqlite_db
        )
        yield builders.GeneModelBuilder(config.gene_model, dependencies.spark_session)
        yield builders.GeneSQLBuilder(
            config.gene_sql, dependencies.spark_session, dependencies.sqlite_db
        )
        yield builders.PrimaryAliquotBuilder(
            config.primary_aliquot,
            dependencies.spark_session,
            dependencies.es_dataframe_util,
        )
        yield builders.ExpressionValueBuilder(
            config.expression_value,
            dependencies.spark_session,
            dependencies.doc_dataframe_util,
        )

        if build.IndexType.GENE_EXPRESSION in index_types:
            yield builders.IndexBuilder(
                config.index,
                dependencies.spark_session,
                dependencies.es_dataframe_util,
                dependencies.mappings_loader,
            )

    @contextlib.contextmanager
    def _initialize_builders(
        self, config: configuration.Configuration, spark_session: sql.SparkSession
    ) -> Iterator[Iterable[builders.Builder]]:
        with self._initialize_dependencies(config, spark_session) as dependencies:
            yield tuple(
                self._builders(
                    config.builders,
                    config.build.index_types,
                    dependencies,
                )
            )


def main() -> None:
    """The main functionality for this driver module."""
    driver.main(Driver())
