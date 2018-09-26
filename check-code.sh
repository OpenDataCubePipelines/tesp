#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

flake8 -j 2 tesp/prepare tests

python -m pytest -r sx --durations=5 "$@"

