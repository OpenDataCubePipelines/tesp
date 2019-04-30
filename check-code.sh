#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

python3 -m pycodestyle tesp --max-line-length 120

python3 -m pylint -j 2 -d line-too-long --reports no tesp

python3 -m pytest tests
