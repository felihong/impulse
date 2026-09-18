# mcp-impulse-agent

A custom MCP server, hosted as a Databricks App, exposing ad-hoc Impulse
queries as agent tools: `list_channels`, `list_containers`,
`preview_histogram`, `preview_histogram_2d`, `preview_stats`, and
`preview_point_values`. See `demos/agent_mcp_query` for the notebook this
was built from, including a full write-up of the design decisions (why not
Genie One / managed MCP, the safety model for virtual-signal expression trees, and
the latency work that got steady-state calls down to 2-7s).

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless compute enabled.
- An already-loaded Impulse silver layer to point this at.
- `databricks` CLI authenticated to your workspace.

## Setup

1. **Build the wheel** — the app installs this repo's `impulse_reporting`/
   `impulse_query_engine` as the `databricks-impulse` dependency (see
   `pyproject.toml`) *and* ships the same wheel to the remote serverless
   workers TSAL compiles Python UDFs onto:
   ```bash
   ./build_wheel.sh
   ```
   Re-run this whenever `src/` changes. It builds from the repo's canonical
   `src/`, so the app never drifts from it. (If the repo `VERSION` bumps,
   update the wheel filename in `pyproject.toml`'s `[tool.uv.sources]` to
   match the name it prints.)

2. **Configure `app.yaml`**: replace the `CATALOG`/`SCHEMA`/`TABLE_PREFIX`
   placeholders with wherever your silver layer lives.

3. **Create and deploy the app**:
   ```bash
   databricks apps create mcp-impulse-agent   # name must start with mcp-
   # the wheel is .gitignored, so force it into the sync with --include
   databricks sync . /Workspace/Users/<you>/mcp-impulse-agent --include 'wheels/*.whl'
   databricks apps deploy mcp-impulse-agent \
     --source-code-path /Workspace/Users/<you>/mcp-impulse-agent
   ```

4. **Grant the app's service principal Unity Catalog access** (client ID
   from `databricks apps get mcp-impulse-agent`):
   ```sql
   GRANT USE CATALOG, BROWSE ON CATALOG <catalog> TO `<service_principal_client_id>`;
   GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA <catalog>.<schema> TO `<service_principal_client_id>`;
   ```

5. **Test it**: the app is automatically discoverable as a custom MCP
   server in AI Playground (Workspace sidebar → Playground → Tools
   dropdown), since its name starts with `mcp-`.

## Consuming from Genie One

**Genie One** uses this app through a **governed Unity Catalog connection**: register the app as
an `HTTP` connection with `is_mcp_connection=true` (metastore-level, M2M auth via a dedicated
service principal that has `CAN_USE` on the app), then add it in Genie One via **+ → More
connections**. The six tools then appear and Genie One draws on them. Full step-by-step — SP,
secret, `CAN_USE`, the connection JSON, and tested prompts — is in [`GENIE_ONE.md`](GENIE_ONE.md).

**Genie Code alternative (no UC connection):** in a **Genie Code** session, **Settings → MCP
Servers → Add Server → Custom MCP servers**, pick `mcp-impulse-agent`, and **Save**. Simpler but
ungoverned.

Two requirements are already handled by this app:

- **Stateless transport.** Genie One requires the server be stateless, so
  the server is created with `FastMCP(..., stateless_http=True)` (see
  `server/main.py`). The `streamable-http` transport still serves the
  standard `/mcp` endpoint, so AI Playground keeps working.
- **Same-workspace deploy.** The app must live in the workspace you connect
  from. If you hit CORS errors, add the workspace URL to the app's allowed
  origins.

Pair the MCP tools with the Impulse **skills** (`skills/` at the repo root,
`SKILL.md` format) installed under
`/Workspace/Users/<you>/.assistant/skills/` so Genie One knows Impulse
vocabulary (channels, containers, TSAL, events) when composing tool calls.
See [`GENIE_ONE.md`](GENIE_ONE.md) for the full setup + tested prompts.

## Implementation notes

- **Dependency pins matter.** `databricks-connect==18.2.*` is required --
  `17.0.1` has a pyspark incompatibility and `18.3.1` is unsupported on
  serverless compute. `databricks-sdk==0.106.0` must match Impulse's own
  pin, since its telemetry code reads a private `Config._product_info`
  attribute that only exists on that version. Don't pin `pyspark`/
  `delta-spark` separately -- let `databricks-connect` provide them.
- **Why the wheel exists at all:** two reasons. (1) TSAL compiles to Python
  UDFs that run on remote serverless workers, not in the app's own process,
  so Impulse has to be shipped there explicitly via
  `DatabricksEnv().withDependencies("local:<wheel>")` -- see
  `server/main.py`'s `get_spark()`. (2) The app's own process installs the
  same wheel as the `databricks-impulse` dependency. Installing it (rather
  than sys.path-referencing bundled source) registers dist-info, so
  `impulse_query_engine` resolves `__version__` via `importlib.metadata`
  instead of falling back to reading a repo-root `VERSION` file -- that
  fallback can't be satisfied in the Databricks Apps layout (source deploys
  under `<app>/source_code/`, but the fallback looks one level above it),
  and the first `Report` build would otherwise fail with
  `FileNotFoundError: '.../VERSION'`.
- **Latency:** AI Playground enforces a ~55s per-call timeout. Persisting
  results to Delta and reading them back took 33-43s per call -- too slow.
  Reading `report.aggregation_dfs[...]` directly (already in final fact
  schema, no join needed) plus going sinkless (`unity_sink` is `Optional`
  on Impulse's `Report` config) cut steady-state latency to 2-7s.

## Known limitations

- **No per-user (on-behalf-of-user) authorization.** All callers share this
  app's single service principal's Unity Catalog permissions. On-behalf-of-
  user auth was attempted and hit a platform inconsistency between the
  Databricks Apps `user_api_scopes` API and the Databricks Connect runtime's
  actual scope requirements -- parked pending further investigation.
- **Distance/custom-weighted histograms are not exposed.** Impulse supports
  weighting histograms by distance or an arbitrary signal
  (`HistogramDistance`/`HistogramCustomWeights`), not just duration.
  Verification found these produce numerically incorrect results (off by
  several orders of magnitude) when the weight signal is derived via
  `resample()`+`cumtrapz()` -- traced to the `synchronized()`+`diff()`
  interaction inside `HistogramCustomWeights.build()` in the query engine,
  not something fixable from this MCP layer. `preview_histogram`/
  `preview_histogram_2d` raise a clear error if you ask for anything other
  than duration weighting, rather than silently returning wrong numbers.
