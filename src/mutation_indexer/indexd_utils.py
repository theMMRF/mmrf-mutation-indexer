import itertools
import logging
from collections.abc import Iterable, Iterator
from typing import NamedTuple

import more_itertools
from indexclient import client
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql import types

DOCUMENT_URL_SCHEMA = types.StructType(
    [
        types.StructField("did", types.StringType()),
        types.StructField("_input_file_name", types.StringType()),
    ]
)


class DocumentUrl(NamedTuple):
    did: str
    url: str


def _is_main_url(metadata: dict):
    """Check if given metadata corresponds to main IndexD URL:
        * type == cleversafe
        * state == validated

    Returns:
        bool: True if main URL, False otherwise
    """
    # return metadata.get("type") == "cleversafe" and metadata.get("state") == "validated"
    return True


def _get_and_format_url(doc: client.Document) -> str | None:
    """Select main IndexD url if one exist and format it to something that Spark
    understands

    Args:
        doc: IndexD document to extract URL from

    Returns:
        str: formatted main URL
    """
    for url, meta in doc.urls_metadata.items():
        if _is_main_url(meta):
            # url = url.replace("s3://", "s3a://").replace("cleversafe.service.consul/", "")
            url = url.replace("s3://", "s3a://")
            return url

    return None


class DataFrameUtil:
    def __init__(
        self,
        indexd: client.IndexClient,
        spark_session: sql.SQLContext | sql.SparkSession,
        logger: logging.Logger,
    ):
        self._indexd = indexd
        self._spark_session = spark_session
        self._logger = logger

    def _get_doc_urls(self, doc_ids: Iterable[str], batch_size: int) -> Iterator[DocumentUrl]:
        batches = more_itertools.ichunked(doc_ids, batch_size)
        docs = itertools.chain.from_iterable(
            self._indexd.bulk_request(list(dids)) or () for dids in batches
        )

        for doc in docs:
            url = _get_and_format_url(doc)

            if url is None:
                self._logger.warning(f"File is missing: '{doc.did}'")

            else:
                yield DocumentUrl(doc.did, url)

    def _get_dataframe(
        self,
        urls: str | Iterable[str],
        schema: types.StructType | None,
        include_file_name: bool,
        enforce_schema: bool,
        has_header: bool,
        comment: str | None,
    ) -> sql.DataFrame:
        urls = list(more_itertools.always_iterable(urls))

        df = self._spark_session.read.csv(
            urls,
            schema=schema,
            inferSchema=True,
            sep="\t",
            comment=comment,
            header=has_header,
            enforceSchema=enforce_schema,
            # mode="FAILFAST",
            mode="PERMISSIVE",
        )

        if include_file_name:
            df = df.withColumn("_input_file_name", F.input_file_name())

        return df

    def get_dataframe(
        self,
        doc_ids: Iterable[str],
        schema: types.StructType | None = None,
        csv_batch_size: int = 500,
        index_batch_size: int = 1000,
        include_document_ids: bool = True,
        enforce_schema: bool = True,
        has_header: bool = True,
        comment: str | None = None,
    ) -> sql.DataFrame:
        doc_urls = tuple(self._get_doc_urls(doc_ids, index_batch_size))
        urls = (doc_url.url for doc_url in doc_urls)
        batches = more_itertools.ichunked(urls, csv_batch_size)

        document_df = self._get_dataframe(
            more_itertools.first(batches, ()),
            schema,
            include_document_ids,
            enforce_schema,
            has_header,
            comment,
        )

        for batch in batches:
            batch_df = self._get_dataframe(
                batch, schema, include_document_ids, enforce_schema, has_header, comment
            )

            document_df = document_df.union(batch_df)

        if include_document_ids:
            url_df = self._spark_session.createDataFrame(doc_urls, schema=DOCUMENT_URL_SCHEMA)

            return document_df.join(url_df, on=["_input_file_name"], how="inner").drop(
                "_input_file_name"
            )

        return document_df
