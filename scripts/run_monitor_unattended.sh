#!/bin/sh
set -eu
set +x
umask 077
case $0 in
  /*) script_path=$0 ;;
  *) exit 2 ;;
esac
script_dir=${script_path%/*}
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
python=$repo_root/.venv/bin/python3
[ -x "$python" ] || exit 2
exec /usr/bin/env -i "$python" -I -m stock_monitor.unattended "$@"
