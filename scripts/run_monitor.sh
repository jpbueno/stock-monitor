#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
cd -- "$repo_root"
if [ -x "$repo_root/.venv/bin/python3" ]; then
    PATH="$repo_root/.venv/bin:$PATH"
    export PATH
fi
exec python3 -m stock_monitor "$@"
