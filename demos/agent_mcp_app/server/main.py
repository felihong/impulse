import operator
import os
import threading
import time
import uuid
from typing import Literal

import pyspark.sql.functions as F
from mcp.server.fastmcp import FastMCP

_spark = None
_spark_lock = threading.Lock()


def _build_spark():
    """Create a fresh serverless Spark session via Databricks Connect.

    Impulse's TSAL layer compiles expressions into Python UDFs that run on
    the remote serverless workers, not in this app's own process -- so the
    bundled impulse_query_engine/impulse_reporting source (not a PyPI
    package) has to be shipped to those workers too via a wheel built from
    the same source (see wheels/), referenced through withDependencies.
    """
    import glob
    from databricks.connect import DatabricksEnv, DatabricksSession
    wheels_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "wheels")
    matches = glob.glob(os.path.join(wheels_dir, "databricks_impulse-*.whl"))
    if not matches:
        raise RuntimeError(
            f"No databricks_impulse-*.whl found in {wheels_dir} -- run build_wheel.sh "
            "(see README) before deploying this app."
        )
    env = DatabricksEnv().withDependencies(f"local:{matches[0]}")
    return DatabricksSession.builder.serverless().withEnvironment(env).getOrCreate()


def get_spark():
    """Return a live serverless Spark session, rebuilding it if the cached one
    has expired.

    A serverless Spark Connect session does not live forever: after enough idle
    time or a hard lifetime cap the remote session is torn down, and the cached
    handle then fails every call with NO_ACTIVE_SESSION. So probe the cached
    session with a trivial query and rebuild it when the probe fails, instead of
    handing back a dead handle. The lock keeps concurrent tool calls from racing
    to build duplicate sessions.
    """
    global _spark
    with _spark_lock:
        if _spark is not None:
            try:
                _spark.sql("SELECT 1").collect()
                return _spark
            except Exception:
                _spark = None  # stale/expired -- fall through and rebuild
        _spark = _build_spark()
        return _spark


# ---------------------------------------------------------------------------
# Lakebase (Postgres) connection -- backs the async job store (submit/poll)
# ---------------------------------------------------------------------------
#
# This app is deployed with a Lakebase "postgres" resource, so the platform
# injects PGHOST/PGPORT/PGDATABASE/PGUSER/PGSSLMODE/LAKEBASE_ENDPOINT at runtime
# (Lakebase connectivity Pattern 4). The DB password is a short-lived (1h) OAuth
# token minted from LAKEBASE_ENDPOINT, so we cache it and re-mint well before
# expiry. Connections are opened per operation (submit, each status write, each
# poll) rather than pooled: traffic is light and short-lived connections sidestep
# both token rotation and the stale-handle problem after a scale-to-zero wake.
_PG_SCHEMA = "impulse_mcp"
_PG_TOKEN_TTL = 2400  # re-mint ~40 min in, comfortably before the 1h token expiry
_pg_token = None
_pg_token_exp = 0.0
_pg_token_lock = threading.Lock()


def _async_enabled() -> bool:
    """True when a Lakebase resource is attached (async submit/poll usable)."""
    return bool(os.environ.get("LAKEBASE_ENDPOINT"))


def _pg_credential() -> str:
    """Return a live Lakebase OAuth token, re-minting it before it expires."""
    global _pg_token, _pg_token_exp
    with _pg_token_lock:
        now = time.time()
        if _pg_token is None or now >= _pg_token_exp:
            from databricks.sdk import WorkspaceClient
            _pg_token = WorkspaceClient().postgres.generate_database_credential(
                endpoint=os.environ["LAKEBASE_ENDPOINT"]
            ).token
            _pg_token_exp = now + _PG_TOKEN_TTL
        return _pg_token


def _pg_connect():
    """Open a fresh Lakebase connection, retrying the ~100ms scale-to-zero wake."""
    import psycopg
    last_err = None
    for attempt in range(3):
        try:
            return psycopg.connect(
                host=os.environ["PGHOST"],
                port=int(os.environ.get("PGPORT", "5432")),
                dbname=os.environ["PGDATABASE"],
                user=os.environ["PGUSER"],
                password=_pg_credential(),
                sslmode=os.environ.get("PGSSLMODE", "require"),
                connect_timeout=30,
            )
        except psycopg.OperationalError as e:
            last_err = e  # transient: scale-to-zero wake or token just rotated
            time.sleep(0.5 * (attempt + 1))
    raise last_err


def _init_jobs_table():
    """Create the app-owned schema + job table on startup (idempotent).

    The app's service principal has CAN_CONNECT_AND_CREATE, so it creates and
    thereby owns this schema -- which is why the app must be deployed before any
    local dev touches the same branch (see the databricks-lakebase skill).
    """
    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_PG_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_PG_SCHEMA}.mcp_jobs (
                job_id      uuid PRIMARY KEY,
                tool        text NOT NULL,
                params      jsonb NOT NULL,
                status      text NOT NULL DEFAULT 'pending',
                result      jsonb,
                error       text,
                created_at  timestamptz NOT NULL DEFAULT now(),
                updated_at  timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        conn.commit()


CATALOG = os.environ["CATALOG"]
SCHEMA = os.environ["SCHEMA"]
TABLE_PREFIX = os.environ["TABLE_PREFIX"]
PFX = f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}"

PORT = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
# stateless_http=True is required for Genie One / Genie Code custom MCP servers
# (each tool call is a self-contained request -- no session id is negotiated or
# stored). The streamable-http transport below still serves the standard /mcp
# endpoint, so AI Playground discovery keeps working unchanged.
mcp = FastMCP("impulse-agent", host="0.0.0.0", port=PORT, stateless_http=True)


def _adhoc_config() -> dict:
    return {
        "source": {
            "container_metrics_table": f"{PFX}_container_metrics",
            "channel_metrics_table": f"{PFX}_channel_metrics",
            "channels_uri": f"{PFX}_channels",
            "container_tags_table": f"{PFX}_container_tags",
            "channel_tags_table": f"{PFX}_channel_tags",
        },
        # No unity_sink: results are read straight from report.aggregation_dfs
        # in memory (see each tool below) and never persisted -- going
        # sinkless also skips several per-call Unity Catalog round trips
        # (gold-layer-exists checks, temp-table cleanup scans) that only
        # matter for persistence.
        "query_engine": {"solver": "DefaultSolver", "data_type": "RAW"},
        "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
    }


def _new_report(name_prefix: str):
    from databricks.sdk import WorkspaceClient
    from impulse_reporting.core.report import Report

    report = Report(
        name=f"{name_prefix}_{uuid.uuid4().hex[:8]}",
        spark=get_spark(), config=_adhoc_config(), workspace_client=WorkspaceClient(),
    )
    return report, report.get_db()


def _aggregation_df(report, agg_type: str):
    """Read an aggregation's result straight from memory (see _adhoc_config)."""
    dfs = report.aggregation_dfs[agg_type]
    return dfs.get("changed") if dfs.get("changed") is not None else dfs["unchanged"]


# ---------------------------------------------------------------------------
# Virtual signal expression trees
# ---------------------------------------------------------------------------

_OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
}

# Whitelist-only binary operators for expression trees. Every node maps to a
# fixed, known Python operator applied to TSAL objects -- there is no
# eval()/exec() of user-provided strings anywhere.
_EXPR_OPS = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "div": operator.truediv,
    "gt": operator.gt,
    "lt": operator.lt,
    "ge": operator.ge,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
    "and": operator.and_,
    "or": operator.or_,
}

# Whitelist-only unary TSAL methods (SampleSeries API) reachable from an
# expression tree's "method" node. Same safety property as _EXPR_OPS: only
# names literally in this set are ever passed to getattr(), so there's no way
# to reach an unintended method (e.g. a dunder) even in principle.
_EXPR_METHODS = {
    # SampleSeries (raw/derived numeric signals)
    "resample", "cumtrapz", "diff", "where",
    "rising_edges", "falling_edges", "rising_edge", "falling_edge",
    "intervals_between_falling_edges", "rolling_average",
    # Intervals (comparison-derived conditions, e.g. channel > threshold) --
    # start_points/end_points are how you turn "RPM is above 2000" into
    # discrete instants ("each time RPM crosses above/below 2000"), as
    # opposed to rising_edges/falling_edges above, which detect transitions
    # in a raw signal that's already boolean-valued (e.g. a brake switch).
    "start_points", "end_points",
}

_MAX_EXPR_DEPTH = 10


def _resolve_arg(arg, db, depth):
    """A method node's args/kwargs may be a literal (number/bool/string) or
    a nested expression node -- resolve the latter recursively."""
    if isinstance(arg, dict) and ({"channel", "const", "op", "method"} & arg.keys()):
        return build_expr(arg, db, depth)
    return arg


def build_expr(node: dict, db, _depth: int = 0):
    """Recursively build a TSAL expression from a constrained JSON tree.

    Node shapes:
      {"channel": "<name>", "tags": {...}}              -- leaf: channel reference
      {"const": <number>}                                -- leaf: constant
      {"op": "<op>", "left": <node>, "right": <node>}    -- one of _EXPR_OPS (binary)
      {"method": "<name>", "operand": <node>,
       "args": [...], "kwargs": {...}}                   -- one of _EXPR_METHODS (unary,
                                                              called on the built operand)

    Every op/method maps to a fixed, known Python operator or TSAL method on
    the same objects list_channels/db.query.channel already produce -- never
    to arbitrary generated code. args/kwargs may themselves be nested
    expression nodes (e.g. the condition passed to a "where" method) or plain
    literals (e.g. the sample_rate passed to "resample").
    """
    if _depth > _MAX_EXPR_DEPTH:
        raise ValueError(f"Expression tree exceeds max depth of {_MAX_EXPR_DEPTH}")
    if "channel" in node:
        return db.query.channel(channel_name=node["channel"], **node.get("tags", {}))
    if "const" in node:
        return node["const"]
    if "method" in node:
        method = node["method"]
        if method not in _EXPR_METHODS:
            raise ValueError(f"Unsupported method {method!r}; must be one of {sorted(_EXPR_METHODS)}")
        if "operand" not in node:
            raise ValueError(f"Method node must have 'operand': {node}")
        operand = build_expr(node["operand"], db, _depth + 1)
        args = [_resolve_arg(a, db, _depth + 1) for a in node.get("args", [])]
        kwargs = {k: _resolve_arg(v, db, _depth + 1) for k, v in node.get("kwargs", {}).items()}
        return getattr(operand, method)(*args, **kwargs)
    if "op" not in node:
        raise ValueError(f"Expression node must have 'channel', 'const', 'method', or 'op': {node}")
    op = node["op"]
    if op not in _EXPR_OPS:
        raise ValueError(f"Unsupported op {op!r}; must be one of {sorted(_EXPR_OPS)}")
    left = build_expr(node["left"], db, _depth + 1)
    right = build_expr(node["right"], db, _depth + 1)
    return _EXPR_OPS[op](left, right)


_EXPR_DOC = """Expression trees (signal_expr / condition_expr / event.signal_expr):
{"channel": "<name>", "tags": {...}} references a channel; {"const": <n>} is a
constant; {"op": "add"|"sub"|"mul"|"div"|"gt"|"lt"|"ge"|"le"|"eq"|"ne"|"and"|"or",
"left": <node>, "right": <node>} combines two nodes (a "gt"/"lt"/etc.
comparison produces an Intervals, e.g. "channel > 2000" means "the time
ranges where this holds", not a per-sample boolean); {"method":
"resample"|"cumtrapz"|"diff"|"where"|"rolling_average", "operand": <node>,
"args": [...]} transforms a numeric signal; {"method":
"start_points"|"end_points", "operand": <node>} turns a comparison's
Intervals into discrete instants (e.g. each time a condition starts/stops
holding); {"method": "rising_edges"|"falling_edges", "operand": <node>} finds
transitions in an already-boolean raw channel (e.g. a brake switch), as
opposed to a comparison. Args may themselves be nested nodes, e.g. the
condition passed to "where". Example -- average of two channels: {"op": "div",
"left": {"op": "add", "left": {"channel": "A"}, "right": {"channel": "B"}},
"right": {"const": 2}}. Example -- cumulative distance from speed: {"method":
"cumtrapz", "operand": {"method": "resample", "operand": {"channel": "Vehicle Speed"},
"args": [1000000]}}."""


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def build_event(event_spec: dict | None, db):
    """Build an Event from a constrained spec dict.

    Shapes:
      None or {"type": "container"} -- ContainerEvent: the whole recording,
        no condition. Default when event is omitted.
      {"type": "basic", "condition_expr": <node>} -- BasicEvent from a
        compound boolean expression tree (see _EXPR_DOC).
      {"type": "basic", "channel_name"/"signal_expr", "tags", "condition_op",
        "condition_value"} -- BasicEvent from a single threshold.
      {"type": "points_in_time", "signal_expr": <node evaluating to
        PointsInTime, typically {"method": "start_points", "operand": <a
        comparison node, e.g. channel > threshold>}>} -- PointsInTimeEvent,
        for discrete instants (pairs with preview_point_values;
        duration-weighted tools like preview_histogram/preview_stats need
        Intervals, not instants).
    """
    from impulse_reporting.events.basic_event import BasicEvent
    from impulse_reporting.events.container_event import ContainerEvent
    from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

    if event_spec is None:
        return ContainerEvent(name="adhoc_event", desc="Full measurement")

    etype = event_spec.get("type", "basic")

    if etype == "container":
        return ContainerEvent(name="adhoc_event", desc="Full measurement")

    if etype == "basic":
        if "condition_expr" in event_spec:
            cond = build_expr(event_spec["condition_expr"], db)
            desc = "custom condition"
        else:
            if "signal_expr" in event_spec:
                channel = build_expr(event_spec["signal_expr"], db)
            elif "channel_name" in event_spec:
                channel = db.query.channel(
                    channel_name=event_spec["channel_name"], **event_spec.get("tags", {})
                )
            else:
                raise ValueError(
                    "basic event needs condition_expr, or (channel_name/signal_expr "
                    "+ condition_op + condition_value)"
                )
            op = event_spec.get("condition_op")
            value = event_spec.get("condition_value")
            if op not in _OPS or value is None:
                raise ValueError(f"condition_op must be one of {list(_OPS)} and condition_value must be set")
            cond = _OPS[op](channel, value)
            desc = f"{event_spec.get('channel_name', 'signal')} {op} {value}"
        return BasicEvent(name="adhoc_event", expr=cond, desc=desc)

    if etype == "points_in_time":
        if "signal_expr" not in event_spec:
            raise ValueError(
                "points_in_time event needs signal_expr (must evaluate to PointsInTime, "
                "e.g. a rising_edges/falling_edges method node)"
            )
        expr = build_expr(event_spec["signal_expr"], db)
        return PointsInTimeEvent(name="adhoc_event", expr=expr, desc="points in time")

    raise ValueError(f"Unsupported event type {etype!r}; must be one of: container, basic, points_in_time")


_EVENT_DOC = """event scopes which time range an aggregation applies to. Omit
for the whole recording. {"type": "basic", "channel_name": "...", "tags": {...},
"condition_op": ">", "condition_value": 2000} scopes to a threshold; {"type":
"basic", "condition_expr": <node>} scopes to a compound boolean condition
(see the expression tree docs). {"type": "points_in_time", "signal_expr": <node>}
scopes to discrete instants (e.g. {"method": "start_points", "operand":
{"op": "gt", "left": {"channel": "Engine RPM"}, "right": {"const": 2000}}}
for each moment RPM crosses above 2000) -- required for preview_point_values,
not usable with duration-weighted tools."""


# ---------------------------------------------------------------------------
# Grounding tools
# ---------------------------------------------------------------------------

@mcp.tool(name="list_channels")
def list_channels_tool() -> list[dict]:
    """List available measurement channels and their tags (e.g. brand, model,
    unit -- whatever tag vocabulary this dataset uses). Read-only and
    instant, with no effect on the report being built -- distinct from
    search_aliases/add_physical_signal, which look up or commit a channel to
    the persisted report definition. Call this to ground yourself before
    preview_histogram/preview_histogram_2d/preview_stats/preview_point_values
    (so channel_name/tags are real values, not guesses), or whenever the user
    asks an exploratory question ("what signals do we have?") without yet
    committing to add anything."""
    spark = get_spark()
    tag_keys = [r["key"] for r in spark.table(f"{PFX}_channel_tags").select("key").distinct().collect()]
    order_col = "channel_name" if "channel_name" in tag_keys else tag_keys[0]
    df = (
        spark.table(f"{PFX}_channel_tags")
        .groupBy("container_id", "channel_id")
        .pivot("key", tag_keys)
        .agg(F.first("value"))
        .select(*tag_keys)
        .distinct()
        .orderBy(order_col)
    )
    return [row.asDict() for row in df.collect()]


@mcp.tool(name="list_containers")
def list_containers_tool() -> list[dict]:
    """List available measurement containers (recording sessions) and their
    tags (e.g. vehicle, duration, condition). Read-only and instant --
    distinct from set_vehicle, which commits a container/time-range to the
    persisted report. Use for questions like "how many test drives do we
    have?" or "what's the total recorded duration?" without adding anything
    to the report."""
    spark = get_spark()
    tag_keys = [r["key"] for r in spark.table(f"{PFX}_container_tags").select("key").distinct().collect()]
    tags_df = (
        spark.table(f"{PFX}_container_tags")
        .groupBy("container_id")
        .pivot("key", tag_keys)
        .agg(F.first("value"))
    )
    df = (
        spark.table(f"{PFX}_container_metrics")
        .join(tags_df, on="container_id", how="left")
        .orderBy("container_id")
    )
    return [row.asDict() for row in df.collect()]


# ---------------------------------------------------------------------------
# Histogram weighting
# ---------------------------------------------------------------------------

def _resolve_weight(weight: dict | None, db):
    """Returns (kind, weights_expr_or_None, extra_kwargs) for a weight spec.
    Only "duration" (the default) is currently supported -- see the
    NotImplementedError message below for why distance/custom are gated off.
    """
    if weight is None:
        return "duration", None, {}
    wtype = weight.get("type", "duration")
    if wtype == "duration":
        return "duration", None, {}
    if wtype in ("distance", "custom"):
        raise NotImplementedError(
            f"weight type {wtype!r} is not supported yet: verification found "
            "HistogramDistance/HistogramCustomWeights produce numerically incorrect "
            "results (off by several orders of magnitude) when the weight signal is "
            "derived via resample()+cumtrapz() -- traced to the synchronized()+diff() "
            "interaction inside the query engine, not something fixable from this MCP "
            "server. Duration weighting (the default -- omit weight, or {\"type\": "
            "\"duration\"}) is fully verified and safe to use."
        )
    raise ValueError(f"weight type must be 'duration' (distance/custom not yet supported), got {wtype!r}")


_WEIGHT_DOC = """weight controls what the histogram accumulates per bin.
Currently only plain duration weighting is supported (time spent in each
bin, in seconds) -- omit weight, or pass {"type": "duration"}. Distance/
custom weighting exists in Impulse but is not exposed here yet pending a
numerical discrepancy fix in the underlying query engine."""


# ---------------------------------------------------------------------------
# Preview tools
# ---------------------------------------------------------------------------

# NOTE: descriptions are built with runtime string concatenation (shared
# _EXPR_DOC/_EVENT_DOC/_WEIGHT_DOC/_SIGNALS_DOC fragments), so they can't live
# as a function docstring -- a first-statement string that isn't a pure literal
# leaves __doc__ = None, and FastMCP would then expose the tool with NO
# description (which breaks tool selection in Genie One / AI Playground). Hoist
# each into a constant and pass it via @mcp.tool(description=...).
_PREVIEW_HISTOGRAM_DESC = (
    """Compute a 1D histogram RIGHT NOW and return the actual numeric result
    -- an instant, read-only preview, not a persisted report aggregation.
    Use this when the user asks a question like "what's the distribution of
    X?" or wants to sanity-check bin edges before committing to anything. Do
    NOT use this in place of add_histogram: add_histogram only registers a
    definition in the wizard's Aggregations step for the report that later
    gets deployed as a job -- this tool runs the computation immediately and
    returns the answer inline, nothing is persisted. A natural flow: call
    this a few times to try different bin choices interactively, then call
    add_histogram with the finalized parameters once the user is happy with
    what they saw here.

    The histogrammed value is either a plain channel (channel_name + optional
    tags to disambiguate, from list_channels) or a derived "virtual signal"
    via signal_expr. """ + _EXPR_DOC + """

    """ + _EVENT_DOC + """

    """ + _WEIGHT_DOC + """

    Provide either (channel_name [+ tags]) or signal_expr. bins_unit/
    values_unit are display labels only (e.g. "rpm") -- the returned value is
    always in seconds regardless of values_unit. Returns one row per bin with
    bin_name, lower_bound, and duration_s."""
)


@mcp.tool(description=_PREVIEW_HISTOGRAM_DESC)
def preview_histogram(
    bins: list[float],
    channel_name: str | None = None,
    tags: dict[str, str] | None = None,
    signal_expr: dict | None = None,
    event: dict | None = None,
    weight: dict | None = None,
    bins_unit: str | None = None,
    values_unit: str = "s",
) -> list[dict]:
    from impulse_reporting.core.page import Page
    from impulse_reporting.aggregations.histogram import HistogramDuration

    report, db = _new_report("adhoc")

    if signal_expr is not None:
        channel = build_expr(signal_expr, db)
    elif channel_name is not None:
        channel = db.query.channel(channel_name=channel_name, **(tags or {}))
    else:
        raise ValueError("Either channel_name or signal_expr must be provided")

    ev = build_event(event, db)
    report.add_event(ev)

    bins_f = [float(b) for b in bins]
    display_name = channel_name or "virtual_signal"
    _resolve_weight(weight, db)  # duration is the only supported kind; raises otherwise

    agg = HistogramDuration(
        name="adhoc_histogram", base_expr=channel, bins=bins_f, event=ev,
        channel_name=display_name, bins_unit=bins_unit or "", values_unit=values_unit,
    )

    page = Page(page_number=1)
    page.add_aggregation(agg)
    report.add_page(page)
    report.determine_report()

    hist_df = _aggregation_df(report, "HISTOGRAM")
    result_df = (
        hist_df
        .groupBy("bin_name", "lower_bound")
        .agg(F.sum("hist_value").alias("duration_us"))
        .orderBy("lower_bound")
        .toPandas()
    )
    result_df["duration_s"] = result_df["duration_us"] / 1e6

    return result_df[["bin_name", "lower_bound", "duration_s"]].to_dict(orient="records")


_PREVIEW_HISTOGRAM_2D_DESC = (
    """Compute a 2D heatmap of two signals (x vs y) RIGHT NOW and return the
    actual numeric result -- an instant, read-only preview, not a persisted
    report aggregation. Use when the user wants to see how two signals
    correlate (e.g. "how does RPM relate to speed while RPM is above 2000?").
    Do NOT use this in place of add_histogram_2d: that only registers a
    definition in the wizard's Aggregations step for the report that later
    gets deployed as a job -- this tool runs the computation immediately and
    returns the answer inline, nothing is persisted.

    x/y axes: provide x_channel_name/y_channel_name (+ optional tags), or
    x_signal_expr/y_signal_expr for derived signals. """ + _EXPR_DOC + """

    """ + _EVENT_DOC + """

    """ + _WEIGHT_DOC + """

    x_bins_unit/y_bins_unit/values_unit are display labels only -- the
    returned value is always in seconds regardless of values_unit. Returns
    one row per (x_bin, y_bin) with x_bin_name, y_bin_name, x_lower_bound,
    y_lower_bound, and duration_s."""
)


@mcp.tool(description=_PREVIEW_HISTOGRAM_2D_DESC)
def preview_histogram_2d(
    x_bins: list[float],
    y_bins: list[float],
    x_channel_name: str | None = None,
    y_channel_name: str | None = None,
    tags: dict[str, str] | None = None,
    x_signal_expr: dict | None = None,
    y_signal_expr: dict | None = None,
    event: dict | None = None,
    weight: dict | None = None,
    x_bins_unit: str | None = None,
    y_bins_unit: str | None = None,
    values_unit: str = "s",
) -> list[dict]:
    from impulse_reporting.core.page import Page
    from impulse_reporting.aggregations.histogram2d import Histogram2DDuration

    report, db = _new_report("adhoc2d")

    if x_signal_expr is not None:
        x_channel = build_expr(x_signal_expr, db)
    elif x_channel_name is not None:
        x_channel = db.query.channel(channel_name=x_channel_name, **(tags or {}))
    else:
        raise ValueError("Either x_channel_name or x_signal_expr must be provided")

    if y_signal_expr is not None:
        y_channel = build_expr(y_signal_expr, db)
    elif y_channel_name is not None:
        y_channel = db.query.channel(channel_name=y_channel_name, **(tags or {}))
    else:
        raise ValueError("Either y_channel_name or y_signal_expr must be provided")

    ev = build_event(event, db)
    report.add_event(ev)

    x_bins_f = [float(b) for b in x_bins]
    y_bins_f = [float(b) for b in y_bins]
    x_name = x_channel_name or "x_signal"
    y_name = y_channel_name or "y_signal"
    _resolve_weight(weight, db)  # duration is the only supported kind; raises otherwise

    agg = Histogram2DDuration(
        name="adhoc_histogram2d", x_expr=x_channel, y_expr=y_channel,
        x_bins=x_bins_f, y_bins=y_bins_f, event=ev,
        x_channel_name=x_name, y_channel_name=y_name,
        x_bins_unit=x_bins_unit, y_bins_unit=y_bins_unit, values_unit=values_unit,
    )

    page = Page(page_number=1)
    page.add_aggregation(agg)
    report.add_page(page)
    report.determine_report()

    hist_df = _aggregation_df(report, "HISTOGRAM2D")
    result_df = (
        hist_df
        .groupBy("x_bin_name", "y_bin_name", "x_lower_bound", "y_lower_bound")
        .agg(F.sum("hist_value").alias("duration_us"))
        .orderBy("x_lower_bound", "y_lower_bound")
        .toPandas()
    )
    result_df["duration_s"] = result_df["duration_us"] / 1e6

    return result_df[
        ["x_bin_name", "y_bin_name", "x_lower_bound", "y_lower_bound", "duration_s"]
    ].to_dict(orient="records")


_SIGNALS_DOC = """signals: list of {"label": "...", "channel_name": "...",
"tags": {...}} (a plain channel) or {"label": "...", "signal_expr": <node>}
(a derived virtual signal) -- label names the signal in the returned rows."""

_STATS = {"min", "max", "mean", "median"}


_PREVIEW_STATS_DESC = (
    """Compute summary statistics for one or more signals RIGHT NOW and
    return the actual numeric result -- an instant, read-only preview, not a
    persisted report aggregation. Use for questions like "what's the average
    X?" or "what were the min/max of X and Y during this event?" -- this is
    the tool for statistics across MULTIPLE signals at once. Do NOT use this
    in place of add_statistics: that only registers a definition in the
    wizard's Aggregations step for the report that later gets deployed as a
    job.

    """ + _SIGNALS_DOC + """
    statistics: which to compute (default: all four -- min, max, mean, median).

    """ + _EVENT_DOC + """

    Statistics are computed per container (a test drive/recording session),
    not collapsed across containers -- averaging an already-averaged value
    across sessions without knowing sample counts wouldn't be statistically
    valid, so each container gets its own row. Returns one row per
    (container_id, label, statistic) with columns container_id, label,
    statistic, value."""
)


@mcp.tool(description=_PREVIEW_STATS_DESC)
def preview_stats(
    signals: list[dict],
    statistics: list[Literal["min", "max", "mean", "median"]] | None = None,
    event: dict | None = None,
) -> list[dict]:
    from impulse_reporting.core.page import Page
    from impulse_reporting.aggregations.stats_aggregator import StatsAggregator

    if not signals:
        raise ValueError("signals must have at least one entry")
    stats = statistics or sorted(_STATS)
    bad = set(stats) - _STATS
    if bad:
        raise ValueError(f"Unsupported statistics {bad}; must be a subset of {sorted(_STATS)}")

    report, db = _new_report("adhoc_stats")

    ev = build_event(event, db)
    report.add_event(ev)

    exprs, labels = [], []
    for sig in signals:
        if "signal_expr" in sig:
            exprs.append(build_expr(sig["signal_expr"], db))
        elif "channel_name" in sig:
            exprs.append(db.query.channel(channel_name=sig["channel_name"], **sig.get("tags", {})))
        else:
            raise ValueError(f"signal {sig!r} needs channel_name or signal_expr")
        labels.append(sig["label"])

    page = Page(page_number=1)
    page.add_aggregation(StatsAggregator(
        name="adhoc_stats", input_expressions=exprs, channel_names=labels,
        statistics=list(stats), event=ev,
    ))
    report.add_page(page)
    report.determine_report()

    stats_df = _aggregation_df(report, "STATS_AGGREGATOR")
    result_df = (
        stats_df
        .select("container_id", "channel_name", "aggregation_label", "statistic_value")
        .orderBy("container_id", "channel_name", "aggregation_label")
        .toPandas()
        .rename(columns={"channel_name": "label", "aggregation_label": "statistic", "statistic_value": "value"})
    )
    return result_df.to_dict(orient="records")


_PREVIEW_POINT_VALUES_DESC = (
    """Sample one or more signals at each instant of a points-in-time event
    RIGHT NOW and return the actual values -- an instant, read-only preview,
    not a persisted report aggregation. Use for questions like "what was the
    speed and RPM at each 10 km milestone?" or "what was the temperature at
    every rising edge of the brake signal?" -- this is the one case
    preview_histogram/preview_stats can't cover, since those need time
    intervals (durations), not discrete instants.

    """ + _SIGNALS_DOC + """

    event: REQUIRED, and must be a points_in_time event -- {"type":
    "points_in_time", "signal_expr": <node evaluating to PointsInTime, e.g.
    {"method": "rising_edges", "operand": {"channel": "Brake Switch"}}>}.

    Returns one row per (container_id, event_instance_id, label) with the
    sampled value."""
)


@mcp.tool(description=_PREVIEW_POINT_VALUES_DESC)
def preview_point_values(
    signals: list[dict],
    event: dict,
) -> list[dict]:
    from impulse_reporting.core.page import Page
    from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator

    if not signals:
        raise ValueError("signals must have at least one entry")
    if not event or event.get("type") != "points_in_time":
        raise ValueError('event must be a points_in_time event: {"type": "points_in_time", "signal_expr": ...}')

    report, db = _new_report("adhoc_pv")

    ev = build_event(event, db)
    report.add_event(ev)

    exprs, labels = [], []
    for sig in signals:
        if "signal_expr" in sig:
            exprs.append(build_expr(sig["signal_expr"], db))
        elif "channel_name" in sig:
            exprs.append(db.query.channel(channel_name=sig["channel_name"], **sig.get("tags", {})))
        else:
            raise ValueError(f"signal {sig!r} needs channel_name or signal_expr")
        labels.append(sig["label"])

    page = Page(page_number=1)
    page.add_aggregation(PointValueAggregator(
        name="adhoc_point_values", input_expressions=exprs, channel_names=labels, event=ev,
    ))
    report.add_page(page)
    report.determine_report()

    pv_df = _aggregation_df(report, "POINT_VALUE_AGGREGATOR")
    result_df = (
        pv_df
        .select("container_id", "event_instance_id", "channel_name", "statistic_value")
        .orderBy("container_id", "event_instance_id", "channel_name")
        .toPandas()
        .rename(columns={"channel_name": "label", "statistic_value": "value"})
    )
    return result_df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Long-running async queries (submit/poll) backed by Lakebase
# ---------------------------------------------------------------------------
#
# Databricks Apps terminate any HTTP request that runs past ~120s, and Genie One
# has no client-native async/streaming for custom MCP tools. So for queries that
# might exceed that budget we expose an app-level async protocol: submit_query
# writes a job row and runs the compute on a background thread, returning a
# job_id in milliseconds; poll_query reads that row until the job is done. Every
# individual HTTP call stays well under the timeout. The fast synchronous
# preview_* tools above are unchanged -- use those for quick questions and this
# pair only when a query is expected to be slow (many fine bins, many channels,
# whole-fleet scans).

_ASYNC_TOOLS = {
    "preview_histogram": preview_histogram,
    "preview_histogram_2d": preview_histogram_2d,
    "preview_stats": preview_stats,
    "preview_point_values": preview_point_values,
}


def _update_job(job_id: str, status: str, result=None, error: str | None = None):
    """Write a job's terminal/intermediate state. Best-effort: a failed write
    leaves the row in its prior state and the poller simply keeps waiting."""
    try:
        with _pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE {_PG_SCHEMA}.mcp_jobs "
                "SET status=%s, result=%s, error=%s, updated_at=now() WHERE job_id=%s",
                (status, result, error, job_id),
            )
            conn.commit()
    except Exception:
        pass


def _run_job(job_id: str, tool: str, arguments: dict):
    """Background worker: run one preview_* computation and record its outcome."""
    from psycopg.types.json import Jsonb
    _update_job(job_id, "running")
    fn = _ASYNC_TOOLS[tool]
    try:
        result = fn(**arguments)
        _update_job(job_id, "done", result=Jsonb(result))
    except Exception as e:  # surfaced to the caller through poll_query
        _update_job(job_id, "error", error=f"{type(e).__name__}: {e}")


_SUBMIT_QUERY_DESC = (
    """Run one of the preview_* computations ASYNCHRONOUSLY, returning a job_id
    immediately instead of waiting for the result inline. Use this INSTEAD of
    calling preview_histogram / preview_histogram_2d / preview_stats /
    preview_point_values directly WHENEVER a query might run longer than ~a
    minute -- e.g. a histogram over many fine bins, statistics across many
    channels, or any aggregation spanning the whole fleet of recordings.
    Databricks Apps cut any single request off at ~120s, so a slow synchronous
    call would fail; this splits the work across a fast submit plus one or more
    quick polls.

    tool: which computation to run -- one of "preview_histogram",
    "preview_histogram_2d", "preview_stats", "preview_point_values".
    arguments: a dict of exactly the keyword arguments that tool takes -- read
    that tool's own description for its parameter contract (bins, signal_expr,
    event, weight, ...). Example: {"tool": "preview_histogram", "arguments":
    {"channel_name": "Engine RPM", "bins": [0, 1000, 2000, 3000],
    "bins_unit": "rpm"}}.

    Returns {"job_id": "...", "status": "pending"} right away. Then call
    poll_query(job_id) every few seconds until status is "done" (result
    attached) or "error". For a question you expect to answer in a second or
    two, skip this and call the preview_* tool directly."""
)


@mcp.tool(description=_SUBMIT_QUERY_DESC)
def submit_query(tool: str, arguments: dict) -> dict:
    import inspect
    from psycopg.types.json import Jsonb

    if not _async_enabled():
        raise RuntimeError(
            "Async queries are unavailable: this app has no Lakebase (postgres) "
            "resource attached. Call the preview_* tools directly instead."
        )
    fn = _ASYNC_TOOLS.get(tool)
    if fn is None:
        raise ValueError(f"Unknown tool {tool!r}; must be one of {sorted(_ASYNC_TOOLS)}")
    try:
        inspect.signature(fn).bind(**arguments)  # fail fast on malformed arguments
    except TypeError as e:
        raise ValueError(f"arguments do not match {tool}'s signature: {e}")

    job_id = str(uuid.uuid4())
    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {_PG_SCHEMA}.mcp_jobs (job_id, tool, params, status) "
            "VALUES (%s, %s, %s, 'pending')",
            (job_id, tool, Jsonb(arguments)),
        )
        conn.commit()
    threading.Thread(target=_run_job, args=(job_id, tool, arguments), daemon=True).start()
    return {"job_id": job_id, "status": "pending"}


_POLL_QUERY_DESC = (
    """Check on an async query started with submit_query -- pass the job_id it
    returned. Returns {"status": "pending"|"running"|"done"|"error", ...}: when
    status is "done" the "result" field holds the same rows the underlying
    preview_* tool would have returned; when "error" the "error" field explains
    what failed. While status is "pending" or "running", wait a few seconds and
    poll again; keep polling until you get "done" or "error"."""
)


@mcp.tool(description=_POLL_QUERY_DESC)
def poll_query(job_id: str) -> dict:
    if not _async_enabled():
        raise RuntimeError("Async queries are unavailable (no Lakebase resource attached).")
    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT status, result, error FROM {_PG_SCHEMA}.mcp_jobs WHERE job_id=%s",
            (job_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"No such job_id {job_id!r}")
    status, result, error = row
    out = {"job_id": job_id, "status": status}
    if status == "done":
        out["result"] = result  # psycopg returns jsonb already parsed to list[dict]
    elif status == "error":
        out["error"] = error
    return out


def _keep_warm():
    """Ping the serverless session periodically so the underlying compute
    doesn't scale down from inactivity between questions. Because get_spark()
    now rebuilds an expired session, this also self-heals a dead session even
    when no tool calls arrive between expiries."""
    while True:
        try:
            get_spark().sql("SELECT 1").collect()
        except Exception:
            pass
        time.sleep(120)


def main():
    import threading
    if _async_enabled():
        try:
            _init_jobs_table()
        except Exception as e:
            # Don't block startup on Lakebase: the sync preview_* tools still
            # work; submit_query/poll_query will report the failure if called.
            print(f"[impulse-mcp] WARNING: could not init Lakebase jobs table: {e}", flush=True)
    threading.Thread(target=_keep_warm, daemon=True).start()
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
