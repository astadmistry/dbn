#! /usr/bin/env bash

source "$(dirname "$0")/config.sh"
grep -E '^version =' "${PROJECT_ROOT_DIR}/rust/dbn/Cargo.toml" | cut -d'"' -f 2
