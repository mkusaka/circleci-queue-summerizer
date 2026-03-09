#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

go run github.com/mkusaka/openapigo/cmd/openapigo@v0.0.0-20260307045818-a8464ffe628a generate -i ./swagger.json -o ./internal/circleciapi -package circleciapi
go run ./scripts/stabilize_generated_types.go -- ./internal/circleciapi/types.go
