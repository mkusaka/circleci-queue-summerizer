#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

curl -fsSL https://circleci.com/api/v2/openapi.json -o swagger.json

python - <<'PY'
from copy import deepcopy
import json

path = "swagger.json"
with open(path) as f:
    spec = json.load(f)

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}

def param(name, description):
    return {
        "in": "path",
        "name": name,
        "description": description,
        "schema": {"type": "string"},
        "required": True,
    }

PROJECT_SLUG_PARAMS = [
    param("provider", "The VCS provider, for example gh."),
    param("organization", "The organization slug."),
    param("project", "The project slug."),
]

ORG_SLUG_PARAMS = [
    param("provider", "The VCS provider, for example gh."),
    param("organization", "The organization slug."),
]

def replace_parameters(params, old_name, new_params):
    out = []
    for p in params or []:
        if p.get("in") == "path" and p.get("name") == old_name:
            out.extend(deepcopy(new_params))
        else:
            out.append(p)
    return out

def patch_path_item(path_item, old_name, new_params):
    if "parameters" in path_item:
        path_item["parameters"] = replace_parameters(path_item.get("parameters"), old_name, new_params)
    for method, op in list(path_item.items()):
        if method in HTTP_METHODS and isinstance(op, dict) and "parameters" in op:
            op["parameters"] = replace_parameters(op.get("parameters"), old_name, new_params)
    return path_item

def merge_path_items(dst, src):
    for key, value in src.items():
        if key == "parameters":
            existing = {(p.get("in"), p.get("name")) for p in dst.get("parameters", [])}
            merged = dst.get("parameters", [])
            for p in value:
                ident = (p.get("in"), p.get("name"))
                if ident not in existing:
                    merged.append(p)
                    existing.add(ident)
            dst["parameters"] = merged
            continue

        if key in HTTP_METHODS:
            if key in dst:
                raise SystemExit(f"conflicting operation while patching paths: {key}")
            dst[key] = value
            continue

        if key not in dst:
            dst[key] = value
    return dst

patched_paths = {}
for original_path, path_item in spec["paths"].items():
    new_path = original_path
    item = deepcopy(path_item)

    if "{project-slug}" in new_path:
        new_path = new_path.replace("{project-slug}", "{provider}/{organization}/{project}")
        item = patch_path_item(item, "project-slug", PROJECT_SLUG_PARAMS)

    if "{org-slug}" in new_path:
        new_path = new_path.replace("{org-slug}", "{provider}/{organization}")
        item = patch_path_item(item, "org-slug", ORG_SLUG_PARAMS)

    if new_path in patched_paths:
        patched_paths[new_path] = merge_path_items(patched_paths[new_path], item)
    else:
        patched_paths[new_path] = item

spec["paths"] = patched_paths

parameters = spec["paths"]["/workflow/{id}/job"]["get"].setdefault("parameters", [])
page_token = {
    "in": "query",
    "name": "page-token",
    "description": "A token to retrieve the next page of results.",
    "schema": {"type": "string"},
    "required": False,
}

if not any(p.get("in") == "query" and p.get("name") == "page-token" for p in parameters):
    parameters.append(page_token)

with open(path, "w") as f:
    json.dump(spec, f, indent=2)
    f.write("\n")
PY

go generate ./...
go mod tidy
