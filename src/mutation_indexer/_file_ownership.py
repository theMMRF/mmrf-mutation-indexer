"""Prepare file ownership, never visibility grants, from a complete IndexD manifest."""

from collections import defaultdict
from copy import deepcopy
import uuid

AUTHZ = "_gen3_file_authz"
VERSION = "_gen3_file_visibility_version"
SUMMARY = "_gen3_file_summary"
SUMMARY_FIELDS = {
    "file_count",
    "file_size",
    "data_categories",
    "experimental_strategies",
}
GUID_FIELDS = {
    "file_id",
    "object_id",
    "did",
    "input_file_id",
    "output_file_id",
    "src_file_id",
}


def resources(value):
    if (
        not isinstance(value, list)
        or not value
        or any(
            not isinstance(v, str) or not v.startswith("/") or v == "/" for v in value
        )
    ):
        raise ValueError(
            "Every file requires nonempty canonical IndexD authz resources"
        )
    return sorted(set(value))


def manifest_records(manifest):
    rows = manifest.get("records") if isinstance(manifest, dict) else manifest
    if not isinstance(rows, list):
        raise ValueError("Provide a complete list of IndexD records")
    result = {}
    identifiers = set()
    for row in rows:
        identifier = row.get("did")
        if (
            not isinstance(identifier, str)
            or not identifier
            or identifier in identifiers
        ):
            raise ValueError("IndexD GUIDs must be present and unique")
        identifiers.add(identifier)
        policy = resources(row.get("authz"))
        aliases = [identifier]
        try:
            aliases.append(str(uuid.UUID(identifier.rsplit("/", 1)[-1])))
        except ValueError:
            pass
        for alias in aliases:
            if alias in result and result[alias] != policy:
                raise ValueError("Ambiguous prefixed file GUID")
            result[alias] = policy
    return result


def file_policy(document, manifest):
    identifiers = [
        value
        for key, value in document.items()
        if key in GUID_FIELDS and isinstance(value, str)
    ]
    if not identifiers:
        return None
    required = set()
    for identifier in identifiers:
        if identifier not in manifest:
            raise ValueError("File GUID missing from the complete IndexD manifest")
        required.update(manifest[identifier])
    return sorted(required)


def summary_groups(files, manifest):
    groups = {}
    seen = set()
    for file in files:
        policy = file.get(AUTHZ) or file_policy(file, manifest)
        if not policy:
            raise ValueError("Cannot summarize an unowned file")
        identifier = file.get("file_id") or file.get("object_id") or file.get("did")
        if not identifier or identifier in seen:
            if identifier in seen:
                continue
            raise ValueError("File summary needs a stable file GUID")
        seen.add(identifier)
        category = file.get("data_category") or []
        strategy = file.get("experimental_strategy") or []
        category = sorted(set(category if isinstance(category, list) else [category]))
        strategy = sorted(set(strategy if isinstance(strategy, list) else [strategy]))
        key = (tuple(policy), tuple(category), tuple(strategy))
        group = groups.setdefault(
            key,
            {
                "authz": policy,
                "file_count": 0,
                "file_size": 0,
                "data_category": category,
                "experimental_strategy": strategy,
                "case_ids": set(),
            },
        )
        group["file_count"] += 1
        size = file.get("file_size") or 0
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("File sizes must be nonnegative integers")
        group["file_size"] += size
        for case in file.get("cases", []):
            if case.get("case_id"):
                group["case_ids"].add(case["case_id"])
    return [
        {**group, "case_ids": sorted(group["case_ids"])}
        for _, group in sorted(groups.items())
    ]


def iter_prepare_sources(documents, manifest, file_documents):
    """Keep cases public; attach ownership to every file and scoped summary group.

    file_documents must contain the complete served file projection so embedded
    case/project summaries can be recomputed without trusting global old totals.
    """
    policies = manifest_records(manifest)
    # Graph file_id and IndexD object_id may identify the same file differently.
    # Resolve graph aliases only through the authoritative object_id lookup.
    for file in file_documents:
        identifier, object_id = file.get("file_id"), file.get("object_id")
        if identifier and object_id and identifier != object_id:
            if object_id not in policies:
                raise ValueError(
                    "File object_id missing from the complete IndexD manifest"
                )
            if identifier in policies and policies[identifier] != policies[object_id]:
                raise ValueError("Ambiguous graph file ownership")
            policies[identifier] = policies[object_id]
    by_case = defaultdict(list)
    by_project = defaultdict(list)
    staged_files = [annotate(file, policies) for file in file_documents]
    for file in staged_files:
        if not file.get(AUTHZ):
            raise ValueError(
                "The file projection contains a document without ownership"
            )
        for case in file.get("cases", []):
            if case.get("case_id"):
                by_case[case["case_id"]].append(file)
            project = case.get("project", {})
            if project.get("project_id"):
                by_project[project["project_id"]].append(file)

    def enrich(document):
        if isinstance(document, list):
            return [enrich(item) for item in document]
        if not isinstance(document, dict):
            return document
        result = {key: enrich(value) for key, value in document.items()}
        if isinstance(result.get("summary"), dict) and SUMMARY_FIELDS.intersection(
            result["summary"]
        ):
            files = None
            if "case_id" in result:
                files = by_case.get(result["case_id"], [])
            elif "project_id" in result:
                files = by_project.get(result["project_id"], [])
            elif isinstance(result.get("files"), list):
                files = result["files"]
            if files is None:
                # Unknown derivatives cannot retain unclassified file totals.
                for field in SUMMARY_FIELDS:
                    result["summary"].pop(field, None)
            else:
                result[SUMMARY] = summary_groups(files, policies)
                expected = result["summary"].get("file_count")
                if expected is not None and expected != sum(
                    row["file_count"] for row in result[SUMMARY]
                ):
                    raise ValueError(
                        "Complete file projection does not match the existing case/project file total"
                    )
        return result

    return (enrich(annotate(document, policies)) for document in documents)


def prepare_sources(documents, manifest, file_documents):
    return list(iter_prepare_sources(documents, manifest, file_documents))


def annotate(document, manifest):
    """Annotate file objects recursively. File references inherit all owners."""
    if not isinstance(document, dict):
        raise ValueError("Each search source must be an object")

    def visit(value):
        if isinstance(value, list):
            return [visit(child) for child in value]
        if not isinstance(value, dict):
            return value
        own = file_policy(value, manifest)
        if "file_name" in value and own is None:
            raise ValueError("File metadata is missing a resolvable GUID")
        result = {
            key: visit(child)
            for key, child in value.items()
            if not key.startswith("_gen3_visibility")
            and not key.startswith("_gen3_file_")
        }

        def loose_refs(child):
            if isinstance(child, str) and child in manifest:
                return {child}
            if isinstance(child, list):
                return (
                    set().union(*(loose_refs(item) for item in child))
                    if child
                    else set()
                )
            return set()

        direct_references = set().union(
            *(loose_refs(child) for child in value.values())
        )
        if own or direct_references:
            required = set(own or [])
            for ref in direct_references:
                required.update(manifest[ref])
            if own:
                # A file revealing private inputs/index files requires their grants too.
                def referenced(child):
                    if isinstance(child, dict):
                        if AUTHZ in child:
                            required.update(child[AUTHZ])
                        for key, item in child.items():
                            if key != "cases":
                                referenced(item)
                    elif isinstance(child, list):
                        for item in child:
                            referenced(item)

                referenced(result)
            result[AUTHZ] = sorted(required)
        return result

    result = visit(deepcopy(document))
    if (
        result.get("project_id")
        and isinstance(result.get("program"), dict)
        and result.get("code")
    ):
        program = result["program"].get("name")
        code = result["code"]
        if (
            not isinstance(program, str)
            or not program
            or "/" in program
            or not isinstance(code, str)
            or "/" in code
        ):
            raise ValueError(
                "Project metadata needs canonical program and project identifiers"
            )
        result[AUTHZ] = [f"/programs/{program}/projects/{code}"]
    result[VERSION] = 1
    return result


def ownership_mapping(documents):
    """Generate additive mappings; every embedded owned object is nested."""
    properties = {
        VERSION: {"type": "integer"},
        AUTHZ: {"type": "keyword"},
        SUMMARY: {"type": "object", "enabled": False},
    }

    def visit(value, props, root=False):
        if isinstance(value, list):
            for item in value:
                visit(item, props, root)
        elif isinstance(value, dict):
            for key, item in value.items():
                if key == SUMMARY:
                    props.setdefault(key, {"type": "object", "enabled": False})
                elif key == AUTHZ:
                    props.setdefault(key, {"type": "keyword"})
                elif (
                    isinstance(item, dict)
                    or isinstance(item, list)
                    and any(isinstance(x, dict) for x in item)
                ):
                    owned = (
                        isinstance(item, dict)
                        and AUTHZ in item
                        or isinstance(item, list)
                        and any(isinstance(x, dict) and AUTHZ in x for x in item)
                    )
                    if key not in props:
                        props[key] = {"properties": {}}
                    if owned:
                        props[key]["type"] = "nested"
                    visit(item, props[key].setdefault("properties", {}))

    for document in documents:
        visit(document, properties, True)
    return {"properties": properties}


def merge_ownership_mapping(mapping, documents):
    """Prepare a fresh index mapping without parent copies of private metadata."""
    return merge_ownership_additions(mapping, ownership_mapping(documents))


def merge_ownership_additions(mapping, additions):
    """Merge validated ownership layout into a fresh index mapping."""
    result = deepcopy(mapping)

    def merge(target, extra):
        for name, definition in extra.items():
            current = target.setdefault(name, {})
            for key, value in definition.items():
                if key == "properties":
                    merge(current.setdefault(key, {}), value)
                else:
                    current[key] = deepcopy(value)

    merge(result.setdefault("properties", {}), additions["properties"])

    def clean(properties, owned=False):
        for name, definition in properties.items():
            child_owned = owned or AUTHZ in definition.get("properties", {})
            if child_owned:
                definition.pop("copy_to", None)
                definition.pop("include_in_parent", None)
                definition.pop("include_in_root", None)
            clean(definition.get("properties", {}), child_owned)
            for multifield in definition.get("fields", {}).values():
                if child_owned:
                    multifield.pop("copy_to", None)

    clean(result["properties"])
    return result


def prepare_build(cases, files, annotations, projects, cached_records):
    """Validate the complete build before any existing index is changed."""
    # A graph node's file_id can differ from IndexD's object_id. Classify both
    # identifiers from the same authenticated IndexD lookup, never graph project_id.
    manifest = {"records": []}
    seen = {}
    for node_id, row in cached_records.items():
        if "error" in row or "ignore" in row:
            continue
        for identifier in (row.get("did"), node_id):
            policy = resources(row.get("authz"))
            if identifier in seen and seen[identifier] != policy:
                raise ValueError("Ambiguous cached file ownership")
            if identifier not in seen:
                seen[identifier] = policy
                manifest["records"].append({"did": identifier, "authz": policy})
    return tuple(
        prepare_sources(documents, manifest, files)
        for documents in (cases, files, annotations, projects)
    )


def indexd_read_auth(enabled, username, password):
    """Require trusted credentials for complete ownership reads when opted in."""
    if not enabled:
        return (None, None)
    if (
        not isinstance(username, str)
        or not username
        or not isinstance(password, str)
        or not password
    ):
        raise ValueError(
            "PROJECT_VISIBILITY_ENABLED requires INDEXD_USERNAME and INDEXD_PASSWORD for complete IndexD reads"
        )
    return username, password
