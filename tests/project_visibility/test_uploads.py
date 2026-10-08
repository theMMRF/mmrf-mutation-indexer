"""Ownership and actual upload-boundary tests; Spark cases also run in CI."""

import ast
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

from mutation_indexer import project_visibility as visibility
from mutation_indexer._file_ownership import AUTHZ, SUMMARY, VERSION

ROOT = Path(__file__).resolve().parents[2]
A, B = "/programs/MMRF/projects/public", "/programs/MMRF/projects/methylation"
MANIFEST = [{"did": "rna", "authz": [A]}, {"did": "meth", "authz": [B]}]
FILES = [
    {
        "file_id": guid,
        "file_name": name,
        "file_size": size,
        "data_category": category,
        "cases": [{"case_id": "case"}],
    }
    for guid, name, size, category in [
        ("rna", "rna.txt", 9, "RNA"),
        ("meth", "meth.txt", 100, "methylation"),
    ]
]
SOURCE = {
    "case_id": "case",
    "clinical": "public",
    "files": FILES,
    "summary": {"file_count": 2, "file_size": 109},
}
MAPPING = {
    "properties": {
        "case_id": {"type": "keyword"},
        "files": {
            "type": "object",
            "include_in_root": True,
            "properties": {"file_name": {"type": "keyword", "copy_to": "all_names"}},
        },
    }
}


class OwnershipTests(unittest.TestCase):
    def test_disabled_does_not_change_rows_mapping_writer_or_require_inputs(self):
        frame, writer = object(), MagicMock()
        with patch.dict(os.environ, {"PROJECT_VISIBILITY_ENABLED": "false"}):
            with visibility.prepared_dataframe(frame, MAPPING) as (result, mapping):
                self.assertIs(result, frame)
                self.assertIs(mapping, MAPPING)
            self.assertIs(visibility.visibility_writer(writer), writer)
            writer.option.assert_not_called()
            visibility.require_indexd_credentials(None, None)

    def test_bad_flag_and_missing_service_credentials_fail_closed(self):
        with patch.dict(os.environ, {"PROJECT_VISIBILITY_ENABLED": "yes"}):
            with self.assertRaises(ValueError):
                visibility.enabled()
        with patch.dict(os.environ, {"PROJECT_VISIBILITY_ENABLED": "true"}):
            with self.assertRaises(ValueError):
                visibility.require_indexd_credentials("", "")
            visibility.require_indexd_credentials("service", "secret")

    def test_partitions_stream_and_preserve_separate_shared_case_ownership(self):
        values = visibility._prepare_partition(
            iter([json.dumps(SOURCE), "invalid-json"]), (MANIFEST, FILES)
        )
        result = json.loads(next(values))
        self.assertNotIn(AUTHZ, result)
        self.assertEqual(result[VERSION], 1)
        self.assertEqual([f[AUTHZ] for f in result["files"]], [[A], [B]])
        self.assertEqual(sum(g["file_size"] for g in result[SUMMARY]), 109)
        with self.assertRaises(ValueError):
            next(values)

    def test_missing_file_owner_rejected_instead_of_public_marker(self):
        with self.assertRaises(ValueError):
            list(
                visibility._prepare_partition(
                    [json.dumps(SOURCE)], (MANIFEST[:1], FILES)
                )
            )

    def test_partition_layout_preserves_clinical_case_and_removes_parent_copies(self):
        values = list(
            visibility._prepare_partition([json.dumps(SOURCE)], (MANIFEST, FILES))
        )
        additions = next(visibility._partition_mapping(values))
        mapping = visibility._merge_additions(MAPPING, additions)
        self.assertEqual(mapping["properties"]["files"]["type"], "nested")
        self.assertNotIn("include_in_root", mapping["properties"]["files"])
        self.assertNotIn(
            "copy_to", mapping["properties"]["files"]["properties"]["file_name"]
        )
        self.assertEqual(MAPPING["properties"]["files"]["type"], "object")


class UploadBoundaryTests(unittest.TestCase):
    def method(self, filename, classname, method):
        # Execute the real method without importing unrelated Spark/model builders.
        module = ast.parse((ROOT / "src/mutation_indexer" / filename).read_text())
        cls = next(
            n
            for n in module.body
            if isinstance(n, ast.ClassDef) and n.name == classname
        )
        fn = next(
            n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == method
        )
        namespace = {
            "project_visibility": visibility,
            "sql": types.SimpleNamespace(DataFrame=object),
            "build": types.SimpleNamespace(IndexType={"CASE_CENTRIC": object}),
            "cast_booleans": lambda frame, mapping: frame,
        }
        exec(
            compile(ast.Module(body=[fn], type_ignores=[]), filename, "exec"), namespace
        )
        return namespace[method]

    def test_both_real_write_methods_prepare_before_create_and_upload(self):
        for path, classname, method in [
            ("es_utils.py", "DataFrameUtil", "write"),
            ("viz/builders/base_builder.py", "BaseBuilder", "load"),
        ]:
            with (
                self.subTest(method=method),
                patch.dict(os.environ, {"PROJECT_VISIBILITY_ENABLED": "true"}),
            ):
                original, prepared = MagicMock(), MagicMock()
                writer = MagicMock()
                writer.option.return_value = writer
                prepared.write.format.return_value = writer
                prepared.coalesce.return_value = prepared
                prepared.repartition.return_value = prepared
                mapper = types.SimpleNamespace(mappings=MAPPING, settings={})
                self_obj = MagicMock()
                self_obj.index_name = "case_centric"
                self_obj.id_field = "case_id"
                self_obj.case_centric = original
                self_obj.config.indices = {"case_centric": "fresh"}
                self_obj.mappings_loader.load_mapper.return_value = mapper
                self_obj._mappings_loader.load_mapper.return_value = mapper
                self_obj.config.es.indices.exists.return_value = False
                self_obj._es_client.indices.exists.return_value = False
                self_obj._get_index.return_value = "fresh"
                events = []
                new_mapping = {"properties": {VERSION: {"type": "integer"}}}

                @contextmanager
                def preparation(frame, mapping):
                    self.assertIs(frame, original)
                    self.assertIs(mapping, MAPPING)
                    events.append("validated")
                    yield prepared, new_mapping

                def create(*args, **kwargs):
                    self.assertEqual(events, ["validated"])
                    self.assertIs(
                        kwargs.get("mappings", args[2] if len(args) > 2 else None),
                        new_mapping,
                    )
                    events.append("created")

                self_obj._create_index.side_effect = create
                self_obj.config.es.indices.create.side_effect = create
                with patch.object(
                    visibility, "prepared_dataframe", side_effect=preparation
                ):
                    fn = self.method(path, classname, method)
                    if method == "write":
                        fn(self_obj, original, object, "case_id")
                    else:
                        fn(self_obj)
                writer.save.assert_called_once_with("fresh")
                self.assertEqual(events, ["validated", "created"])
                writer.option.assert_any_call("es.spark.dataframe.write.null", False)

    def test_both_write_methods_reject_existing_destination_before_preparation(self):
        for path, classname, method in [
            ("es_utils.py", "DataFrameUtil", "write"),
            ("viz/builders/base_builder.py", "BaseBuilder", "load"),
        ]:
            obj = MagicMock()
            obj.index_name = "case_centric"
            obj.config.indices = {"case_centric": "existing"}
            obj._es_client.indices.exists.return_value = True
            obj.config.es.indices.exists.return_value = True
            with (
                patch.dict(os.environ, {"PROJECT_VISIBILITY_ENABLED": "true"}),
                patch.object(visibility, "prepared_dataframe") as prepare,
            ):
                fn = self.method(path, classname, method)
                with self.assertRaisesRegex(ValueError, "fresh"):
                    if method == "write":
                        fn(obj, MagicMock(), object, "case_id")
                    else:
                        fn(obj)
                prepare.assert_not_called()
                obj.config.es.indices.create.assert_not_called()
                obj._create_index.assert_not_called()


@unittest.skipUnless(
    importlib.util.find_spec("pyspark"), "Run with Python 3.13, Spark 3.5 and Java 17"
)
class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("file-ownership-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def inputs(self, directory):
        manifest, files = (
            Path(directory) / "manifest.json",
            Path(directory) / "files.ndjson",
        )
        manifest.write_text(json.dumps(MANIFEST))
        files.write_text("".join(json.dumps(f) + "\n" for f in FILES))
        return {
            "PROJECT_VISIBILITY_ENABLED": "true",
            "PROJECT_VISIBILITY_INDEXD_MANIFEST": str(manifest),
            "PROJECT_VISIBILITY_FILE_DOCUMENTS": str(files),
        }

    def test_actual_distributed_dataframe_preserves_types_rows_and_ownership(self):
        frame = self.spark.read.json(
            self.spark.sparkContext.parallelize(
                [
                    json.dumps(SOURCE),
                    json.dumps({"case_id": "other", "clinical": "public"}),
                ],
                2,
            )
        )
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, self.inputs(d)):
            with visibility.prepared_dataframe(frame, MAPPING) as (prepared, mapping):
                rows = {
                    row["case_id"]: row
                    for row in (json.loads(x) for x in prepared.toJSON().collect())
                }
                self.assertEqual(set(rows), {"case", "other"})
                self.assertNotIn(AUTHZ, rows["case"])
                self.assertEqual(rows["case"]["files"][1][AUTHZ], [B])
                self.assertEqual(sum(g["file_count"] for g in rows["case"][SUMMARY]), 2)
                self.assertEqual(rows["case"]["clinical"], "public")
                self.assertEqual(mapping["properties"]["files"]["type"], "nested")
                self.assertEqual(
                    prepared.schema["files"].dataType.elementType["file_size"].dataType,
                    frame.schema["files"].dataType.elementType["file_size"].dataType,
                )

    def test_invalid_row_fails_before_upload_context_is_entered(self):
        frame = self.spark.read.json(
            self.spark.sparkContext.parallelize(
                [json.dumps({"file_id": "not-in-manifest", "file_name": "hidden"})]
            )
        )
        entered = False
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, self.inputs(d)):
            with self.assertRaises(Exception):
                with visibility.prepared_dataframe(frame, MAPPING):
                    entered = True
        self.assertFalse(entered)

    def test_empty_output_has_valid_mapping_and_no_invented_records(self):
        from pyspark.sql import types as T

        frame = self.spark.createDataFrame(
            [], T.StructType([T.StructField("case_id", T.StringType())])
        )
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, self.inputs(d)):
            with visibility.prepared_dataframe(frame, MAPPING) as (result, mapping):
                self.assertEqual(result.count(), 0)
                self.assertEqual(mapping["properties"][VERSION], {"type": "integer"})

    def test_dynamic_map_ownership_cannot_be_lost_during_schema_projection(self):
        from pyspark.sql import types as T

        schema = T.StructType(
            [T.StructField("references", T.MapType(T.StringType(), T.StringType()))]
        )
        with self.assertRaises(ValueError):
            visibility._extended_schema(
                schema,
                {
                    "properties": {
                        "references": {"properties": {AUTHZ: {"type": "keyword"}}}
                    }
                },
                True,
            )


if __name__ == "__main__":
    unittest.main()
