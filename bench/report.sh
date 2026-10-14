#!/usr/bin/env bash
# Regenerates bench/results/README.md and its charts from bench/results/raw/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
docker run --rm -v "$ROOT:/w" -w /w python:3.12-slim sh -c \
  'pip install --quiet --disable-pip-version-check --root-user-action=ignore matplotlib==3.9.2 && python bench/report.py bench/results'
