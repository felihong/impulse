# Consuming the Impulse MCP server from Genie One

This is the end-to-end runbook for standing up the Impulse ad-hoc query MCP server
(`demos/agent_mcp_app/`) as a live Databricks App and driving it from **Genie One** — with the
Impulse **skills** installed so Genie One speaks the domain vocabulary. It is the consumption
story that replaces the earlier bespoke app-UI frontend: no custom frontend, just Genie One + a
governed Unity Catalog MCP connection + skills. **Genie Code**, a separate surface, can also use
the app directly (see the alternative in §4.1).

It doubles as the write-up of a real rebuild on the `fevm-hongzhu` workspace
(`hongzhu_aws_workspace_catalog.impulse`), including the three code changes that make the
server Genie-One-ready and the actual tool-call results captured against the running app.

---

## 1. Architecture

```
┌──────────────────────┐   silver layer     ┌───────────────────────────┐
│ agent_mcp_query.ipynb │ ──── loads ─────▶  │ hongzhu_aws_workspace_    │
│ (demo notebook)       │   5 CSV tables     │ catalog.impulse.agent_*   │
└──────────────────────┘                    └────────────┬──────────────┘
                                                          │ reads
                                             ┌────────────▼──────────────┐
   Genie One                                 │ mcp-impulse-agent          │
   ┌───────────────────────┐   MCP /mcp      │ (Databricks App, FastMCP)  │
   │ chat + skills          │ ───tools──────▶ │ 6 tools; builds Impulse    │
   │ (.assistant/skills)    │ ◀──results───── │ Reports, runs TSAL on      │
   └───────────────────────┘                 │ serverless, returns inline │
                                              └────────────────────────────┘
```

- **Notebook** (`demos/agent_mcp_query.ipynb`) loads a self-contained silver layer and proves
  the tool logic in-process.
- **App** (`demos/agent_mcp_app/`) is the same six tools as a deployable FastMCP server. It
  builds a fresh Impulse `Report` per call, runs the TSAL on serverless, and returns the
  answer inline (no persisted gold table).
- **Genie One** connects to the app as a **custom MCP server** and calls the tools; the
  installed skills give it Impulse vocabulary.

The six tools: `list_channels`, `list_containers` (discovery), and `preview_histogram`,
`preview_histogram_2d`, `preview_stats`, `preview_point_values` (ad-hoc compute).

---

## 2. What changed to make it Genie-One-ready

Three changes on top of the original AI-Playground-only server:

1. **Stateless transport** (`server/main.py`). Genie One requires a stateless MCP server, so
   the server is created with `FastMCP(..., stateless_http=True)`. `mcp.run(
   transport="streamable-http")` still serves the standard `/mcp` endpoint, so AI Playground
   keeps working. (Verified: the server returns no `mcp-session-id` header.)

2. **Real tool descriptions** (`server/main.py`). The four `preview_*` tools composed their
   docstrings with runtime string concatenation (`"""…""" + _EXPR_DOC + """…"""`). A first
   statement that isn't a *pure* string literal leaves `__doc__ = None`, so FastMCP exposed
   those tools with **empty descriptions** — fatal for Genie One's tool selection. Each
   description is now hoisted into a module constant and passed via
   `@mcp.tool(description=...)`. (Verified: descriptions went from `desc_len=0` to 1013–3476
   chars.)

3. **Install Impulse from its wheel, not bundled source** (`pyproject.toml`, `build_wheel.sh`).
   `impulse_query_engine.__init__` resolves `__version__` from installed dist-info and, failing
   that, reads a repo-root `VERSION` file. In the Databricks Apps layout the app source deploys
   under `<app>/source_code/`, so that fallback looks at `<app>/VERSION` — one level above where
   sync can put a file, so the first `Report` build died with
   `FileNotFoundError: '/app/python/VERSION'`. The app now installs `databricks-impulse` from
   the locally built wheel (`[tool.uv.sources]`), which registers dist-info so the
   `importlib.metadata` path succeeds. It's the same wheel `get_spark()` ships to the serverless
   workers, so app process and workers run identical code.

---

## 3. Rebuild from scratch

Prereqs: Unity Catalog + serverless enabled, `databricks` CLI authenticated. Below uses
profile `fevm-hongzhu`, catalog `hongzhu_aws_workspace_catalog`, schema `impulse`, table
prefix `agent`.

### 3.1 Create the schema and load the silver layer

```bash
databricks schemas create impulse hongzhu_aws_workspace_catalog --profile fevm-hongzhu
```

Import the repo into the workspace (as **files**, so the notebook can import `src/`), then run
`demos/agent_mcp_query.ipynb` with widgets `catalog=hongzhu_aws_workspace_catalog`,
`schema=impulse`, `table_prefix=agent`:

```bash
databricks sync . /Workspace/Users/<you>/impulse --profile fevm-hongzhu --full
```

> **Serverless environment gotcha.** Impulse requires Python 3.11+ (it uses `StrEnum`/`Self`).
> Serverless **Environment Version 1 ships Python 3.10** and the first Impulse import fails
> (`ImportError: cannot import name 'StrEnum'`). Run the notebook on **Environment Version 2+**
> (pick it in the notebook's Environment panel). The data-load cells (which only use
> pandas + Spark) run on any version. This is also documented in `skills/impulse/SKILL.md`.

This creates `hongzhu_aws_workspace_catalog.impulse.agent_{container_metrics,container_tags,
channel_metrics,channel_tags,channels}`.

### 3.2 Build and deploy the MCP app

```bash
cd demos/agent_mcp_app
./build_wheel.sh                      # builds wheels/databricks_impulse-<ver>.whl

# app.yaml: set CATALOG=hongzhu_aws_workspace_catalog, SCHEMA=impulse, TABLE_PREFIX=agent
databricks apps create mcp-impulse-agent --profile fevm-hongzhu   # name must start with mcp-

# sync forcing in the wheel (it's .gitignored, so pass --include)
databricks sync . /Workspace/Users/<you>/mcp-impulse-agent \
  --profile fevm-hongzhu --full --include 'wheels/*.whl'

databricks apps deploy mcp-impulse-agent \
  --source-code-path /Workspace/Users/<you>/mcp-impulse-agent --profile fevm-hongzhu
```

### 3.3 Grant the app's service principal UC access

Client id from `databricks apps get mcp-impulse-agent`:

```sql
GRANT USE CATALOG, BROWSE ON CATALOG hongzhu_aws_workspace_catalog TO `<sp_client_id>`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE
  ON SCHEMA hongzhu_aws_workspace_catalog.impulse TO `<sp_client_id>`;
```

(`MODIFY`/`CREATE TABLE` are needed because each ad-hoc call writes to a throwaway scratch
prefix that it cleans up inline.)

### 3.4 Smoke-test the endpoint

```bash
TOKEN=$(databricks auth token --profile fevm-hongzhu | jq -r .access_token)
URL="https://<app-host>.databricksapps.com/mcp"
curl -sS -X POST "$URL" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

You should get six tools, all with non-empty `description`.

---

## 4. Wire it into Genie One

### 4.1 Register the app as a Unity Catalog MCP connection (Genie One)

Genie One consumes the app through a **governed Unity Catalog connection** — the app registered as
an `HTTP` connection with `is_mcp_connection=true`, authenticating M2M as a dedicated service
principal.

1. **Dedicate a service principal** (e.g. `impulse-mcp-sp`) and generate an OAuth secret for it
   (its page → **Secrets → Generate secret**). Note its client ID (`application_id`).
2. **Grant that SP `CAN_USE` on the app** — otherwise tool calls fail with `401`:

   ```bash
   databricks apps update-permissions mcp-impulse-agent --profile fevm-hongzhu --json '{
     "access_control_list": [
       {"service_principal_name": "<sp-client-id>", "permission_level": "CAN_USE"}
     ]
   }'
   ```
3. **Create the connection at the metastore level** (schema-level connections do not surface in
   Genie One's picker):

   ```bash
   databricks connections create --profile fevm-hongzhu --json '{
     "name": "impulse_mcp_conn",
     "connection_type": "HTTP",
     "options": {
       "host": "https://<app-host>.databricksapps.com",
       "port": "443",
       "base_path": "/mcp",
       "is_mcp_connection": "true",
       "oauth_scope": "all-apis",
       "token_endpoint": "https://<workspace-host>/oidc/v1/token",
       "client_id": "<sp-client-id>",
       "client_secret": "<sp-oauth-secret>"
     }
   }'
   ```
4. **Add it in Genie One:** open Genie One → **+** (bottom-left of the search bar) → **More
   connections** → select `impulse_mcp_conn`. The six tools appear (allow / ask / deny per tool);
   anyone using it needs `USE CONNECTION` on the connection.

The MCP toggle on the Catalog Explorer HTTP form is gated behind the workspace preview **Third
Party Connectors for Agents**; the CLI above sets `is_mcp_connection=true` regardless, which is why
it's the reliable route.

**Genie Code alternative (no UC connection).** In a **Genie Code** session you can add the app
directly instead: **Settings → MCP Servers → Add Server → Custom MCP servers**, select
`mcp-impulse-agent`, and **Save**. Simpler but ungoverned. Either path needs the app in the **same
workspace**, reachable at `https://<host>/mcp` in **stateless** mode (add the workspace URL to the
app's allowed origins if you hit CORS).

### 4.2 Install the Impulse skills

The skills teach Genie One the Impulse domain (channels, containers, events, TSAL). From the repo
root:

```bash
databricks workspace import-dir skills \
  /Workspace/Users/<you>/.assistant/skills --profile fevm-hongzhu --overwrite
```

Genie One picks skills up on next use; they're on by default and can be toggled on the
customization page. Confirm with: *"List the Impulse skills you can use."*

---

## 5. Test prompts for Genie One

Ask these in order, easiest → hardest for the model, and watch which tool it calls:

| # | Prompt | Expected tool |
|---|--------|---------------|
| 1 | "What measurement channels and vehicles do we have?" | `list_channels` / `list_containers` |
| 2 | "Show the distribution of Engine RPM in 1000-rpm bins." | `preview_histogram` |
| 3 | "What fraction of time was Engine RPM above 3000?" | `preview_histogram` (sum top bins ÷ total) |
| 4 | "Average and max RPM while vehicle speed was above 100 km/h?" | `preview_stats` + basic event |
| 5 | "How does Engine RPM relate to Vehicle Speed? Give me a heatmap." | `preview_histogram_2d` |
| 6 | "What were speed and RPM at each moment RPM crossed above 3000?" | `preview_point_values` + points-in-time event |
| 7 | "Distribution of the gap between ambient and intake air temperature." | `preview_histogram` + virtual signal (`sub`) |

---

## 6. Verified tool results (live server, `fevm-hongzhu`)

Captured by calling the deployed app's `/mcp` directly (exactly the JSON-RPC Genie One sends).
Cold start of the serverless session is ~50s on the first compute call; warm calls are
~2–11s.

**`list_channels`** → 16 rows. Distinct channels: `Ambient Air Temperature`, `DTC`,
`DTC_count`, `Engine RPM`, `Intake Air Temperature`, `Vehicle Speed Sensor`; tags include
`brand=Seat`, `model=Leon`, route (`from_city`/`to_city`), `unit`.

**`preview_histogram`** (Engine RPM, 1000-rpm bins) → dwell time (seconds) per bin:

| bin (rpm) | duration_s |
|-----------|-----------:|
| 0–1000 | 2 241.6 |
| 1000–2000 | 8 414.2 |
| 2000–3000 | 3 296.5 |
| 3000–4000 | 76.0 |
| 4000–5000 | 0.0 |
| 5000–6000 | 0.0 |

→ time above 3000 rpm = 76.0 / (sum) ≈ **0.5%**.

**`preview_stats`** (Engine RPM, whole recording, container 1): min 0, **max 3385**,
mean 1570.0, median 1714. (Returns one row per container × statistic — 24 rows for 2 signals.)

**`preview_histogram_2d`** (Engine RPM × Vehicle Speed Sensor) → 12 (x_bin, y_bin) cells with
duration_s; e.g. RPM 0–2000 × speed 0–50 km/h = 5 766.9 s, the dominant cell.

**`preview_point_values`** (speed + rpm at each `start_points` of RPM > 3000) → 14 rows; at the
crossing instants rpm ≈ 3001–3026 and speed ≈ 176–178 km/h — physically consistent.

**Virtual signal** (`preview_histogram` of `Ambient − Intake` air temp, `signal_expr` with
`op: sub`) → distribution centered in the −10–0 °C bin (11 421.7 s), i.e. intake air is
usually a little warmer than ambient. Confirms the safe expression-tree path works end to end.

---

## 7. Known limitations

- **Shared service principal.** All Genie One callers share the app SP's UC permissions; there's
  no per-user (on-behalf-of-user) authorization yet.
- **Duration weighting only.** Distance/custom-weighted histograms are intentionally disabled
  (a known upstream numerical issue); the tools raise a clear error rather than returning wrong
  numbers.
- **Serverless Environment Version.** The notebook needs Environment Version 2+ (Python 3.11+);
  the app is unaffected (it pins Python 3.12 in its own venv).
- **Going live on the docs site** requires the upstream PR to merge, since
  databrickslabs.github.io/impulse builds from `databrickslabs/impulse`.
