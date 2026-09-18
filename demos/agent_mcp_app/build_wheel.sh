#!/usr/bin/env bash
# Builds a wheel of Impulse (impulse_reporting/impulse_query_engine) from this
# repo's own src/ into ./wheels/. The app uses that wheel in two places:
#
#   1. Its own process installs it as the `databricks-impulse` dependency (see
#      pyproject.toml's [tool.uv.sources]). Installing it -- rather than
#      sys.path-referencing bundled source -- gives it dist-info, so
#      impulse_query_engine resolves __version__ via importlib.metadata instead
#      of the source-only VERSION-file fallback (which can't be satisfied in the
#      Databricks Apps layout).
#   2. get_spark() ships the same wheel to the remote serverless workers via
#      DatabricksEnv().withDependencies("local:<wheel>") -- TSAL compiles to
#      Python UDFs that run there, not in the app's own process.
#
# So app process and workers run byte-identical Impulse code.
#
# Run this once before `databricks sync` + `databricks apps deploy`, and again
# whenever the repo's src/ changes. Assumes this directory lives two levels
# under the repo root (e.g. demos/agent_mcp_app/) -- adjust REPO_ROOT if moved.
#
# NOTE: pyproject.toml pins the wheel filename by version
# (databricks_impulse-<VERSION>-py3-none-any.whl). If the repo's VERSION bumps,
# update that path in [tool.uv.sources] to match the name printed below.
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="../.."

echo "Building wheel from $REPO_ROOT..."
rm -f wheels/databricks_impulse-*.whl
mkdir -p wheels
python3 -m pip install -q build
python3 -m build --wheel --outdir wheels "$REPO_ROOT"
echo "Built: $(ls wheels/databricks_impulse-*.whl)"
echo "Ensure pyproject.toml [tool.uv.sources] references the filename above."
