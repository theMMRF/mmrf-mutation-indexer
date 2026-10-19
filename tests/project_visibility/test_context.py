"""Semantic equivalence and bounded-input-memory regressions."""

from copy import deepcopy
import json
import os
from pathlib import Path
import pickle
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

from mutation_indexer import project_visibility as visibility
from mutation_indexer._file_ownership import AUTHZ, SUMMARY, iter_prepare_sources
from mutation_indexer.visibility_context import build_context, iter_prepare_context

A, B = "/programs/MMRF/projects/public", "/programs/MMRF/projects/methylation"
MANIFEST = [{"did": "rna", "authz": [A]}, {"did": "meth", "authz": [B]}]
FILES = [
    {
        "file_id": guid,
        "file_name": guid + ".txt",
        "file_size": size,
        "data_category": "RNA" if guid == "rna" else ["methylation"],
        "experimental_strategy": ["test"],
        "cases": [
            {"case_id": case, "project": {"project_id": "project"}}
            for case in ("case-1", "case-2")
        ],
    }
    for guid, size in (("rna", 9), ("meth", 100))
]


class ContextTests(unittest.TestCase):
    def test_identical_ownership_and_summaries_for_shared_cases_and_projects(self):
        documents = [
            {
                "case_id": "case-1",
                "clinical": "public",
                "files": FILES,
                "summary": {"file_count": 2, "file_size": 109},
            },
            {"project_id": "project", "summary": {"file_count": 2}},
            {"summary": {"file_count": 2}, "files": FILES},
            {"summary": {"file_count": 500, "file_size": 20000}, "clinical": "public"},
            {"case_id": "case-2", "src_file_id": "meth"},
            {"arbitrary_reference": ["meth"], "nested": {"file_id": "rna"}},
        ]
        context = build_context(MANIFEST, lambda: iter(FILES))
        original = pickle.dumps(context)
        self.assertEqual(
            list(iter_prepare_context(documents, context)),
            list(iter_prepare_sources(documents, MANIFEST, FILES)),
        )
        self.assertEqual(pickle.dumps(context), original)
        summary = context.by_project["project"]
        self.assertEqual(sum(row["file_count"] for row in summary), 2)
        self.assertEqual(sum(row["file_size"] for row in summary), 109)

    def test_later_graph_alias_and_private_dependencies_survive_compaction(self):
        files = deepcopy(FILES)
        files[0]["inputs"] = [{"file_id": "graph-meth"}]
        files[1].update(file_id="graph-meth", object_id="meth")
        context = build_context(MANIFEST, lambda: iter(files))
        documents = [{"case_id": "case-1", "summary": {"file_count": 2}}, *files]
        result = list(iter_prepare_context(documents, context))
        self.assertEqual(result, list(iter_prepare_sources(documents, MANIFEST, files)))
        self.assertEqual(result[1][AUTHZ], sorted([A, B]))
        self.assertTrue(
            any(row["authz"] == sorted([A, B]) for row in result[0][SUMMARY])
        )

    def test_bad_metadata_unknown_owner_and_duplicate_inputs_fail_closed(self):
        for files in (
            [dict(FILES[0], file_id="missing")],
            [dict(FILES[0], file_size=-1)],
            [dict(FILES[0], extra={"file_name": "unclassified"})],
            [FILES[0], FILES[0]],
        ):
            with self.subTest(files=files), self.assertRaises(ValueError):
                build_context(MANIFEST, lambda: iter(files))
        context = build_context(MANIFEST, lambda: iter(FILES))
        with self.assertRaises(ValueError):
            list(iter_prepare_context([{"src_file_id": "missing"}], context))
        with self.assertRaises(ValueError):
            list(
                iter_prepare_context(
                    [{"case_id": "case-1", "summary": {"file_count": 3}}], context
                )
            )

    def test_partition_does_not_rebuild_or_copy_complete_inputs(self):
        context = build_context(MANIFEST, lambda: iter(FILES))
        with patch.object(
            visibility, "build_context", side_effect=AssertionError("rebuild")
        ):
            rows = list(
                visibility._prepare_partition(
                    [json.dumps({"src_file_id": "meth"})], context
                )
            )
        self.assertEqual(json.loads(rows[0])[AUTHZ], [B])

    def inputs(self, directory, manifest=MANIFEST, files=FILES):
        manifest_path, files_path = (
            Path(directory) / "manifest.json",
            Path(directory) / "files.ndjson",
        )
        manifest_path.write_text(json.dumps(manifest))
        with files_path.open("w") as stream:
            for file in files:
                stream.write(json.dumps(file) + "\n")
        return {
            "PROJECT_VISIBILITY_INDEXD_MANIFEST": str(manifest_path),
            "PROJECT_VISIBILITY_FILE_DOCUMENTS": str(files_path),
        }

    def test_context_reused_across_outputs_and_recompiled_if_inputs_change(self):
        visibility._cached_inputs.cache_clear()
        with tempfile.TemporaryDirectory() as directory:
            env = self.inputs(directory)
            with (
                patch.dict(os.environ, env),
                patch.object(
                    visibility, "build_context", wraps=build_context
                ) as compile_context,
            ):
                first = visibility._load_inputs()
                self.assertIs(first, visibility._load_inputs())
                self.assertEqual(compile_context.call_count, 1)
                with Path(env["PROJECT_VISIBILITY_FILE_DOCUMENTS"]).open("a") as stream:
                    stream.write("\n")
                self.assertIsNot(first, visibility._load_inputs())
                self.assertEqual(compile_context.call_count, 2)
        visibility._cached_inputs.cache_clear()

    def test_sidecar_peak_memory_does_not_scale_with_unused_payload(self):
        visibility._cached_inputs.cache_clear()
        manifest = [{"did": str(i), "authz": [A]} for i in range(200)]
        files = (
            {
                "file_id": str(i),
                "file_name": "test.txt",
                "file_size": 1,
                "cases": [{"case_id": "case", "project": {"project_id": "project"}}],
                "unused_clinical_payload": "x" * 65536,
            }
            for i in range(200)
        )
        with tempfile.TemporaryDirectory() as directory:
            env = self.inputs(directory, manifest, files)
            size = Path(env["PROJECT_VISIBILITY_FILE_DOCUMENTS"]).stat().st_size
            with patch.dict(os.environ, env):
                tracemalloc.start()
                try:
                    context = visibility._load_inputs()
                    _, peak = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
            self.assertLess(peak, size // 2)
            self.assertLess(len(pickle.dumps(context)), 20000)
            self.assertNotIn(b"unused_clinical_payload", pickle.dumps(context))
            self.assertEqual(context.by_case["case"][0]["file_count"], 200)
        visibility._cached_inputs.cache_clear()


if __name__ == "__main__":
    unittest.main()
