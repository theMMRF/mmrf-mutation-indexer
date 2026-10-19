"""Compile complete file inputs into bounded ownership and summary lookups."""

from collections import defaultdict
from typing import NamedTuple

from mutation_indexer._file_ownership import (
    AUTHZ,
    SUMMARY,
    SUMMARY_FIELDS,
    annotate,
    manifest_records,
    summary_groups,
)


class OwnershipContext(NamedTuple):
    policies: dict
    by_case: dict
    by_project: dict


def _add(groups, row):
    key = (
        tuple(row["authz"]),
        tuple(row["data_category"]),
        tuple(row["experimental_strategy"]),
    )
    group = groups.setdefault(key, dict(row, file_count=0, file_size=0, case_ids=set()))
    group["file_count"] += row["file_count"]
    group["file_size"] += row["file_size"]
    group["case_ids"].update(row["case_ids"])


def _finish(lookup):
    return {
        identifier: [
            dict(row, case_ids=sorted(row["case_ids"]))
            for _, row in sorted(groups.items())
        ]
        for identifier, groups in lookup.items()
    }


def build_context(manifest, files):
    """Read a repeatable file iterator twice; retain no full file documents.

    The first pass resolves graph aliases before any dependent file is validated.
    The second validates each complete source against the authoritative manifest,
    then retains only grouped summary statistics. Shared policies are interned.
    """
    policies = manifest_records(manifest)
    for file in files():
        identifier, object_id = file.get("file_id"), file.get("object_id")
        if identifier and object_id and identifier != object_id:
            if object_id not in policies:
                raise ValueError(
                    "File object_id missing from the complete IndexD manifest"
                )
            if identifier in policies and policies[identifier] != policies[object_id]:
                raise ValueError("Ambiguous graph file ownership")
            policies[identifier] = policies[object_id]

    pool = {}
    for identifier, policy in policies.items():
        policies[identifier] = pool.setdefault(tuple(policy), policy)

    by_case, by_project = defaultdict(dict), defaultdict(dict)
    seen = set()
    for source in files():
        file = annotate(source, policies)
        if not file.get(AUTHZ):
            raise ValueError(
                "The file projection contains a document without ownership"
            )
        identifier = file.get("file_id") or file.get("object_id") or file.get("did")
        if not identifier or identifier in seen:
            raise ValueError(
                "The complete file projection requires unique stable GUIDs"
            )
        seen.add(identifier)
        row = summary_groups([file], policies)[0]
        case_ids, project_ids = set(), set()
        for case in file.get("cases") or []:
            if case.get("case_id"):
                case_ids.add(case["case_id"])
            project = case.get("project") or {}
            if project.get("project_id"):
                project_ids.add(project["project_id"])
        for identifier in case_ids:
            _add(by_case[identifier], row)
        for identifier in project_ids:
            _add(by_project[identifier], row)
    return OwnershipContext(policies, _finish(by_case), _finish(by_project))


def iter_prepare_context(documents, context):
    """Apply the same ownership rules using precomputed complete summaries."""
    policies, by_case, by_project = context

    def enrich(document):
        if isinstance(document, list):
            return [enrich(item) for item in document]
        if not isinstance(document, dict):
            return document
        result = {key: enrich(value) for key, value in document.items()}
        if isinstance(result.get("summary"), dict) and SUMMARY_FIELDS.intersection(
            result["summary"]
        ):
            groups = None
            if "case_id" in result:
                groups = by_case.get(result["case_id"], [])
            elif "project_id" in result:
                groups = by_project.get(result["project_id"], [])
            elif isinstance(result.get("files"), list):
                groups = summary_groups(result["files"], policies)
            if groups is None:
                for field in SUMMARY_FIELDS:
                    result["summary"].pop(field, None)
            else:
                result[SUMMARY] = groups
                expected = result["summary"].get("file_count")
                if expected is not None and expected != sum(
                    row["file_count"] for row in groups
                ):
                    raise ValueError(
                        "Complete file projection does not match the existing case/project file total"
                    )
        return result

    return (enrich(annotate(document, policies)) for document in documents)
