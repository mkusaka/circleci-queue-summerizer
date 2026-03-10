#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

go run github.com/mkusaka/openapigo/cmd/openapigo@v0.0.0-20260309134111-9b35d9b2aa8f generate -i ./swagger.json -o ./internal/circleciapi -package circleciapi
