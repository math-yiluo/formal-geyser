"""On-demand submission formats in ordinary, unpublished workspace files."""
from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path

from .coordination.content import PRIVATE_DIRECTORY
from .coordination.runtime.files import ScopedFiles
from .schemas import OBJECT_TYPES
from .semantic_review_reference import semantic_review_example, semantic_review_schema

REFERENCE_DIRECTORY = f"{PRIVATE_DIRECTORY}/taskhub-reference"
WORKER_RECORD_TYPES = ("definition", "statement", "proof")


def submission_schema(object_type: str) -> dict:
    """Describe one native manifest, keeping authoritative semantic validation in Python."""
    if object_type not in WORKER_RECORD_TYPES:
        raise ValueError("This record type is not available for worker publication")
    record = deepcopy(OBJECT_TYPES[object_type].model_json_schema())
    definitions = record.pop("$defs", {})
    filled = {"object_type"}
    if object_type in {"definition", "statement", "proof"}:
        filled.update({"bundle", "environment_artifact", "environment_version"})
        definitions["LeanMarkdownBundle"]["required"] = []
    if object_type in {"proof", "build-candidate"}:
        filled.update({"task_id", "task_revision", "snapshot", "environment_artifact"})
    if object_type == "build-candidate":
        filled.add("modules")
    record["required"] = [name for name in record.get("required", []) if name not in filled]
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "additionalProperties": False,
        "required": ["kind", "data", "introduction"],
        "properties": {
            "kind": {"const": "lean." + object_type}, "data": record,
            "introduction": {"type": "string", "minLength": 1, "maxLength": 4000},
            # What integration accepts (Integration._publication_paths). A limit of 200 here made a
            # worker in the Linux pilot plan three candidates for one 456-file port.
            "repository_paths": {"type": "array", "items": {"type": "string"}, "maxItems": 10000},
            "input_refs": {"type": "array", "items": {"type": "string"}, "maxItems": 200},
            "coauthors": {"type": "array", "items": {"type": "string"}, "maxItems": 32,
                          "description": "Optional: existing workers of this instance who contributed."},
            "replaces": {"type": "string", "minLength": 1},
            "policy": {"type": "string", "description": "Normally omitted; if supplied it must match the exact domain policy."},
        },
    }
    if definitions:
        schema["$defs"] = definitions
    return schema


@lru_cache(maxsize=1)
def _reference_files() -> tuple[tuple[str, str], ...]:
    guide = Path(__file__).with_name("submission-guide.md").read_text(encoding="utf-8")
    review_guide = Path(__file__).with_name("semantic-review-guide.md").read_text(encoding="utf-8")
    return (("README.md", guide), ("review/README.md", review_guide),
            ("review/schema.json", json.dumps(semantic_review_schema(), ensure_ascii=False, indent=2) + "\n"),
            ("review/example.json", json.dumps(semantic_review_example(), ensure_ascii=False, indent=2) + "\n"), *(
        (f"schemas/native/{name}.json", json.dumps(submission_schema(name), ensure_ascii=False, indent=2) + "\n")
        for name in WORKER_RECORD_TYPES))


def prepare_reference(workspace: Path) -> dict[str, str]:
    """Refresh generated reference files safely; never include them in a submission."""
    files = ScopedFiles(workspace)
    for relative, text in _reference_files():
        name = f"{REFERENCE_DIRECTORY}/{relative}"
        target = files.path(name, must_exist=False)
        if target.is_file() and target.read_bytes() == text.encode("utf-8"):
            continue
        files.write(name, text)
    # Workspace-relative paths work with native tools and the scoped fallback.
    return {"submission_guide": f"{REFERENCE_DIRECTORY}/README.md",
            "submission_schemas": f"{REFERENCE_DIRECTORY}/schemas/native",
            "semantic_review_guide": f"{REFERENCE_DIRECTORY}/review/README.md",
            "semantic_review_schema": f"{REFERENCE_DIRECTORY}/review/schema.json",
            "semantic_review_example": f"{REFERENCE_DIRECTORY}/review/example.json"}
