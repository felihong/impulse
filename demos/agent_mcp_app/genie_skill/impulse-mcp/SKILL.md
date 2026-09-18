---
name: impulse-mcp
description: >
  Use when an Impulse MCP server (the `mcp-impulse-agent` Databricks App) is connected to this
  Genie session and the user asks an ad-hoc, quantitative question about time-series measurement
  data — test drives, sensor/telemetry channels, "what fraction of time", "distribution of",
  "average/min/max", "how does X relate to Y", "value at each …". Prefer the MCP tools over writing
  Impulse Python: they build and run the query immediately and return the numeric answer inline,
  with no gold-layer table to define first. Pair with the `impulse` / `impulse-tsal` /
  `impulse-analyze` skills for the underlying vocabulary (channels, containers, events, signals).
---

# Impulse via MCP — answering ad-hoc measurement questions with tools

The connected `mcp-impulse-agent` server exposes six read-only tools over an Impulse silver layer.
They compute results **right now** (no persisted report). Reach for them whenever the user asks a
quantitative question about recordings/channels; do **not** hand-write `impulse_reporting` Python for
these — that path is for building *persisted* gold-layer reports, not one-off answers.

## Always ground first

Before composing any `preview_*` call, call the discovery tools so channel names and tag values are
**real**, not guessed:

- **`list_channels`** — distinct channels + their tag vocabulary (e.g. `brand`, `model`, `unit`).
- **`list_containers`** — recording sessions + their tags (vehicle, condition, route, duration).

Use the exact `channel_name` strings these return, and pass `tags` only to disambiguate when the same
channel name appears under multiple tag combinations.

## Pick the tool by question shape

| The user is asking…                                                       | Tool                    |
|---------------------------------------------------------------------------|-------------------------|
| "distribution of X", "what fraction of time above/below…", "histogram of…"| `preview_histogram`     |
| "how does X relate to Y", a heatmap / 2D distribution of two signals      | `preview_histogram_2d`  |
| "average / min / max / median of X (and Y…)", summary statistics          | `preview_stats`         |
| "value of X at each <instant>" (every milestone, every rising edge, …)    | `preview_point_values`  |

Each tool's own description carries the full parameter contract (bins, the `signal_expr` expression
grammar, the `event` scope grammar, and histogram `weight`). Read the tool description rather than
guessing; the notes below are just orientation.

## Composing the arguments

- **Plain channel vs. virtual signal.** For a raw channel, pass `channel_name` (+ `tags` if needed).
  For a derived signal (e.g. "power = torque × rpm", or a comparison), pass a `signal_expr`
  expression tree instead — a nested dict of whitelisted ops (`add/sub/mul/div`, `gt/lt`) and methods
  (`resample`, `cumtrapz`, `diff`, `where`, edge detection). Provide **one or the other**, not both.
- **Scoping with `event`.** Omit `event` to aggregate over the whole recording. Provide a `basic`
  event (a boolean condition, e.g. Engine RPM > 2000) to restrict to matching intervals, or — for
  `preview_point_values` — a `points_in_time` event (e.g. rising edges of a signal) that defines the
  discrete instants to sample at.
- **Units are labels.** `bins_unit` / `values_unit` are display strings only; histogram values are
  returned in **seconds** of dwell time regardless.

## Interpreting results

- Histograms return one row per bin (`bin_name`, `lower_bound`, `duration_s`). Turn `duration_s` into
  a *fraction of time* by dividing by the total across bins when the user asks "what fraction".
- `preview_stats` returns **one row per container** (per recording), not a single collapsed number —
  averaging pre-averaged values across sessions isn't statistically valid, so summarize per container
  or state the range.
- If a tool raises on distance/custom-weighted histograms, that weighting is intentionally disabled
  (known upstream numerical issue) — fall back to duration weighting.

## A typical turn

1. `list_channels` → confirm "Engine RPM", "Vehicle Speed Sensor" exist and their tags.
2. `preview_histogram(channel_name="Engine RPM", bins=[0,1000,2000,3000,4000,5000,6000], bins_unit="rpm")`.
3. Report the dwell-time distribution; if asked "what fraction above 3000", sum the top bins ÷ total.
