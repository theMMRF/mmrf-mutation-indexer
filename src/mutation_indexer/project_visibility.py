"""Opt-in ownership preparation at every Elasticsearch upload boundary."""

from contextlib import contextmanager
from copy import deepcopy
import json
import os
from pathlib import Path

from mutation_indexer._file_ownership import (
    AUTHZ,
    SUMMARY,
    VERSION,
    iter_prepare_sources,
    manifest_records,
    merge_ownership_additions,
    ownership_mapping,
)


def enabled():
    value = os.getenv("PROJECT_VISIBILITY_ENABLED", "false")
    if value not in ("true", "false"):
        raise ValueError("PROJECT_VISIBILITY_ENABLED must be true or false")
    return value == "true"


def require_indexd_credentials(user, password):
    if enabled() and (not user or not password):
        raise ValueError(
            "Project visibility requires trusted IndexD service credentials"
        )


def _load_inputs():
    manifest_path = os.getenv("PROJECT_VISIBILITY_INDEXD_MANIFEST")
    files_path = os.getenv("PROJECT_VISIBILITY_FILE_DOCUMENTS")
    if not manifest_path or not files_path:
        raise ValueError(
            "Set PROJECT_VISIBILITY_INDEXD_MANIFEST and PROJECT_VISIBILITY_FILE_DOCUMENTS"
        )
    manifest = json.loads(Path(manifest_path).read_text())
    files = [
        json.loads(line)
        for line in Path(files_path).read_text().splitlines()
        if line.strip()
    ]
    manifest_records(manifest)
    # Validate ownership and aliases before broadcasting or creating any index.
    list(iter_prepare_sources((), manifest, files))
    return manifest, files


def _prepare_partition(values, inputs):
    manifest, files = inputs
    for source in iter_prepare_sources(
        (json.loads(value) for value in values), manifest, files
    ):
        yield json.dumps(source, separators=(",", ":"))


def _partition_mapping(values):
    yield ownership_mapping(json.loads(value) for value in values)


def _merge_additions(left, right):
    return merge_ownership_additions(left, right)


def _contains_policy(layout):
    properties = layout.get("properties", {})
    return (
        AUTHZ in properties
        or SUMMARY in properties
        or any(_contains_policy(child) for child in properties.values())
    )


def _extended_schema(schema, layout, root=False):
    from pyspark.sql import types as T

    if isinstance(schema, T.ArrayType):
        return T.ArrayType(
            _extended_schema(schema.elementType, layout), schema.containsNull
        )
    if isinstance(schema, T.MapType):
        if _contains_policy(layout):
            raise ValueError(
                "Ownership in dynamic Spark maps is unsupported; normalize file objects to structs"
            )
        return schema
    if not isinstance(schema, T.StructType):
        return schema
    properties = layout.get("properties", {})
    fields = [
        T.StructField(
            field.name,
            _extended_schema(field.dataType, properties.get(field.name, {})),
            field.nullable,
            deepcopy(field.metadata),
        )
        for field in schema.fields
        if field.name not in (AUTHZ, SUMMARY, VERSION)
    ]
    if root:
        fields.append(T.StructField(VERSION, T.IntegerType(), True))
    if AUTHZ in properties:
        fields.append(T.StructField(AUTHZ, T.ArrayType(T.StringType()), True))
    if SUMMARY in properties:
        summary = T.StructType(
            [
                T.StructField("authz", T.ArrayType(T.StringType())),
                T.StructField("file_count", T.LongType()),
                T.StructField("file_size", T.LongType()),
                T.StructField("data_category", T.ArrayType(T.StringType())),
                T.StructField("experimental_strategy", T.ArrayType(T.StringType())),
                T.StructField("case_ids", T.ArrayType(T.StringType())),
            ]
        )
        fields.append(T.StructField(SUMMARY, T.ArrayType(summary), True))
    return T.StructType(fields)


@contextmanager
def prepared_dataframe(df, mapping):
    """Validate all rows before index creation; stream partitions rather than collect.

    The original Spark types and row IDs are retained. Complete immutable inputs
    are broadcast once per output, and the validated JSON is cached on executor
    disks until upload completes. No downstream dataframe schema is changed.
    """
    if not enabled():
        yield df, mapping
        return
    from pyspark import StorageLevel

    inputs = df.sparkSession.sparkContext.broadcast(_load_inputs())
    prepared = None
    try:
        prepared = (
            df.toJSON()
            .mapPartitions(lambda values: _prepare_partition(values, inputs.value))
            .persist(StorageLevel.DISK_ONLY)
        )
        additions = prepared.mapPartitions(_partition_mapping).fold(
            ownership_mapping(()), _merge_additions
        )
        updated_mapping = merge_ownership_additions(mapping, additions)
        schema = _extended_schema(df.schema, additions, root=True)
        result = df.sparkSession.read.schema(schema).json(prepared)
        yield result, updated_mapping
    finally:
        if prepared is not None:
            prepared.unpersist()
        inputs.destroy()


def visibility_writer(writer):
    # Suppress null ownership fields on public structs only in opted-in uploads.
    return (
        writer.option("es.spark.dataframe.write.null", False) if enabled() else writer
    )
