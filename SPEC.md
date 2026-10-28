# SPEC: redpanda-connect-event-time-cutover

A Redpanda Connect processor plugin, `event_time_cutover`, implementing the
**Event-Time Handoff**: move a live consumer from an old source to a new source
that carries the same events, with no lost or duplicated events, no offset
resets, no coordination and no shared state.

Priorities, in order: **correct, simple, easy to read, robust.**
No clever abstractions. Small files, clear names, comments that explain *why*.

Module: `github.com/GolfRider/redpanda-connect-event-time-cutover`
License: Apache 2.0. Unofficial community plugin for Redpanda Connect.

---

## 1. Terms

| Term | Meaning |
|---|---|
| source | Where a message came from (usually a topic name) |
| lane | `from` (old sources), `to` (new sources), `passthrough` (not migrating) |
| ts | Event time in epoch milliseconds, written by the producer |
| cut | One switch entry: `FROM@<time>` or `TO@<time>` |
| switch | Ordered history of cuts, e.g. `FROM@0,TO@T1,FROM@T2` |
| group | A set of event types that share one switch |
| owner(ts) | The lane (`from` or `to`) that serves events with that ts |

## 2. Invariants (the user must guarantee these; README states them first)

1. Both sources carry the same events.
2. Each event has the **same ts** in both copies. ts is the producer's event
   time, never the broker arrival time.
3. If groups are used, each event has the **same event type** in both copies.
4. Everything placed before this processor treats both copies the same way
   (see section 9).

## 3. Ownership rule

```
owner(ts) = lane of the last cut whose time <= ts
served    = (message lane == owner(ts))      # for from/to lanes
```

Each copy decides from its own ts, so exactly one copy is served, whatever the
lag, arrival order or partition owner.

Examples:

| Switch | Meaning |
|---|---|
| `FROM@0` | old owns everything; new is counted and dropped (dry run) |
| `FROM@0,TO@T1` | new owns ts >= T1; old owns ts < T1 |
| `FROM@0,TO@T1,FROM@T2` | rollback: old owns ts < T1 and ts >= T2; new owns [T1, T2) |

Keeping the full history makes rollback exact: late events with ts < T1 still
belong to old after a rollback.

## 4. Switch format

```
SWITCH := CUT ("," CUT)*
CUT    := ("FROM" | "TO") "@" TIME
TIME   := "0" | EPOCH_MS | RFC3339 | SIMPLE
SIMPLE := YYYY "-" MM "-" DD " " HH ":" MM " " ZONE
ZONE   := "UTC" | "GMT" | "Z" | ("+" | "-") HH ":" MM | IANA | ALIAS
ALIAS  := "PT" | "MT" | "CT" | "ET"
```

- `EPOCH_MS`: exactly 13 digits.
- `RFC3339`: must include an explicit offset (`Z` or `+05:30`, `-08:00`).
- `SIMPLE`: the preferred human form, 24-hour, minute precision, wall-clock
  time in the given zone. Single spaces only. Examples:
  `2026-11-15 14:30 PT`, `2026-11-15 14:30 Asia/Kolkata`,
  `2026-11-15 14:30 UTC`, `2026-11-15 14:30 -08:00`.
- `IANA`: a tz database name (`America/Los_Angeles`). Daylight saving time is
  applied for the given date, so ops never pick an offset by hand. Embed the
  database with `import _ "time/tzdata"` so it works in minimal containers.
- `ALIAS`: US shorthands that follow daylight saving time:
  `PT` = `America/Los_Angeles`, `MT` = `America/Denver`,
  `CT` = `America/Chicago`, `ET` = `America/New_York`. Case-sensitive.
- `GMT` is accepted as `UTC`: it is always +00:00 and has no daylight saving.
- Reject a local time that does not exist or occurs twice on a daylight
  saving change day (e.g. 02:30 on spring-forward, 01:30 on fall-back);
  Go would silently pick one.
- Every time must carry an explicit zone. Reject times without one (pods
  usually run in UTC; a naive local time silently means a different moment).
- Reject zone abbreviations (`PST`, `PDT`, `CST`, `IST`): they are ambiguous,
  people write `PST` in summer, and Go parses unknown abbreviations as
  offset 0 without an error. No 12-hour clock (am/pm mistakes shift the cut
  by 12 hours).
- Normalize every TIME to epoch ms at parse time.
- Every TIME other than the literal `0`, in any form, must fall in [1e12, 1e13)
  (2001-09-09 .. 2286-11-20), the event ts range of section 6. A cut outside
  it could never match a valid event, so it can only be a typo.
- Surrounding whitespace around cuts is allowed and trimmed. Keywords are
  uppercase only.

Validation (parse error -> instance refuses to start):
- empty string is invalid (an unset env var becomes empty)
- first cut must be `FROM@0`
- cuts alternate `FROM` / `TO`
- cut times strictly increasing
- any malformed TIME

Do **not** fail at startup because a cut time is in the past; restarts after
the cut must work. "T must be in the future" is a deploy-time check (section 12).

## 5. Processor config

Simple form (all events share one switch):

```yaml
- event_time_cutover:
    switch: "${CUTOVER_SWITCH}"
    from: [orders_old]
    to: [orders_new]
    passthrough: [other_events]
    source: 'meta("kafka_topic")'
    timestamp: 'this.event_ts'
```

Group form (selective events, independent switches):

```yaml
- event_time_cutover:
    from: [topic-a]
    to: [topic-b]
    passthrough: [other_events]
    source: 'meta("kafka_topic")'
    timestamp: 'this.event_ts'
    event_type: 'this.type'
    cutovers:
      - name: e12
        events: [event1, event2]
        switch: "${CUTOVER_SWITCH_E12}"
      - name: rest
        events: ["*"]
        switch: "FROM@0"
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `switch` | string | one of `switch` / `cutovers` | Shorthand for a single group `events: ["*"]` named `default` |
| `cutovers` | list of {name, events, switch} | one of `switch` / `cutovers` | |
| `from` | []string | yes, non-empty | |
| `to` | []string | yes, non-empty | |
| `passthrough` | []string | no | |
| `source` | Bloblang | no, default `meta("kafka_topic")` | Works with any input, not only Kafka |
| `timestamp` | Bloblang | yes | Must yield the event ts |
| `event_type` | Bloblang | required if any group is not `["*"]` | Also used as metrics label |

Startup validation (error -> refuse to start):
- both `switch` and `cutovers` set, or neither
- a source in more than one lane; `from` or `to` empty
- an event type in more than one group; more than one `"*"` group
- duplicate or empty group names; a group with no events
- any invalid switch; every group's switch is built with `cutswitch.Parse`,
  never left as the zero value (which silently behaves like `FROM@0`)

## 6. Event timestamp parsing

The `timestamp` mapping result may be:
- a JSON number or an integer string -> epoch ms
- an RFC 3339 string with offset -> converted to epoch ms

Reject (message error, reason `invalid_ts`):
- missing / null, non-numeric, fractional numbers, negative values
- values outside [1e12, 1e13) — catches seconds-instead-of-ms bugs
- uint64 values that overflow int64

Parse with care for int64 vs uint64; do not go through float64 for integers.

## 7. Message handling (decision order)

1. Evaluate `source`. Find its lane.
    - `passthrough` -> keep message unchanged, count, done.
    - no lane -> mark error `unknown_source`, keep message, done.
2. Evaluate `timestamp`. Invalid -> mark error `invalid_ts`, keep, done.
3. Find group: by `event_type` if groups need it, else the `"*"` group.
   No match -> mark error `unknown_event_type`, keep, done.
4. `served = (lane == group.switch.owner(ts))`.
    - served -> keep message.
    - not served -> drop message.
5. Record metrics.

Errors: use the standard Redpanda Connect message error flag (not a custom
DLQ). Users route errors with the usual patterns, e.g. an output `switch` with
`check: errored()` to a DLQ output, or `reject_errored`. Never silently drop
bad input. README shows a complete DLQ example.

Implement as a plain per-message processor; no batching logic needed.

## 8. Metrics and logs

- counter `cutover_routed{group, source, event_type, served}`
  (`served` = "true" / "false"; `event_type` = "unknown" if not configured)
- counter `cutover_errors{reason}` (`unknown_source`, `invalid_ts`, `unknown_event_type`)
- gauge `cutover_cut_info{group, cut_ts, owner}` = 1, set at startup.
  `cut_ts` = time of the last cut (epoch ms); `owner` = lane of the last cut.
  Operators check that each group has exactly one series across all instances.

Startup log, one line per group: name, events, every cut as epoch ms **and**
UTC, and the current owner.

README notes: keep `event_type` low-cardinality; a stale `cut_info` series can
linger briefly after a rollout; partitions with no commits show flat full lag.

## 9. Placement (README section)

The processor goes anywhere a processor is allowed; its position in the list
decides what runs before it.

Safe before it: content-based filters (type, field values), decoding
(JSON, Avro, Protobuf), extracting ts.

Not safe before it: anything that depends on topic, partition, offset or arrival
time; anything that changes ts or event type; stateful processors (`dedupe`,
caches) that may drop one copy; processors that merge or split messages.

## 10. Package layout

```
/internal/cutswitch     pure: parse, validate, Owner(ts), Describe(); no Redpanda Connect imports
/internal/cutover       pure: config model, validation, Decide(source, ts, eventType) -> Decision
/processor              plugin glue: ConfigSpec, constructor, Process(), metrics, logs
/cmd/redpanda-connect-cutover   custom binary: standard components + this processor
/cmd/cutover-switch     small ops tool (section 12)
/examples               configs: simple, groups, DLQ routing
/deploy                 docker-compose with Redpanda for local end-to-end
```

Use the public plugin API (`github.com/redpanda-data/benthos/v4/public/service`).
Check the current API in the module source / godoc rather than guessing. For the
custom binary, prefer the Apache-licensed component bundle from
`github.com/redpanda-data/connect/v4`; verify the correct import path.

### Upstream-ready design

Goal: ship as a standalone module **and** be easy to contribute to Redpanda
Connect. Upstream would copy the code into its tree, not import this module
(`internal/` is not importable, and small external deps are unwelcome).

- `cutswitch` and `cutover` stay pure (standard library only), so they copy
  over unchanged apart from import paths.
- `processor` uses only the public `service` API, as upstream components do.
- `processor` exports `Register(env *service.Environment) error` instead of
  registering in `init()`. The custom binary calls it explicitly (no globals,
  no panics); upstream wraps it in its usual `init()`.
- User docs live in the ConfigSpec (descriptions, field docs, examples),
  because upstream generates its docs from it. The README can repeat them.
- Mark the processor as experimental/beta in its spec.
- Before M3, read upstream's CONTRIBUTING, component layout, metric naming
  and test conventions; follow them where they don't conflict with this spec.
- Likely review points, each removable on its own: the embedded `time/tzdata`
  (~450 KB) and the US zone aliases (`PT`, `MT`, `CT`, `ET`).

## 11. Tests

- `cutswitch`: table tests for valid / invalid switches (every rule in section 4),
  RFC 3339 with and without offset, SIMPLE form with `UTC` / `Z` / offset /
  IANA name / alias (winter and summer dates), DST gap and overlap times,
  rejected zone abbreviations and am/pm, 13-digit boundaries, whitespace.
- Ownership boundaries: ts == T, ts == T-1, multiple cuts, rollback history.
- **Property test**: for any valid switch and any ts, exactly one of
  {from copy, to copy} is served. Run per group.
- Timestamp parsing: numbers, digit strings, RFC 3339, uint64 overflow,
  seconds-not-ms, negatives, fractions, null.
- Processor: passthrough, unknown source, invalid ts, unknown event type,
  mixed-type batches, metrics labels.
- End-to-end (docker-compose): dry run -> cut -> rollback, verifying counts.

## 12. Ops tool: `cutover-switch`

```
cutover-switch explain "FROM@0,TO@2026-11-15T02:00:00-08:00"
cutover-switch check   "$CUTOVER_SWITCH" --min-lead 30m
```

- `explain`: prints each cut as epoch ms, UTC and local time, and who owns
  which ranges.
- `check`: fails if the newest cut is in the past or closer than `--min-lead`.
  Meant for CI / pre-deploy, not for runtime.

## 13. Rollout (README section)

1. Ship the processor with `FROM@0`, old source only. Nothing changes.
   Verify one `cut_info` series per group.
2. Add the new source (dry run). Start it with a fresh consumer group from
   before any possible ts >= T (earliest or a safe timestamp, never "latest").
   Compare counts per event type over the same window; check CPU and memory.
3. Set `FROM@0,TO@<T>`, one environment at a time. T = rollout time + margin.
   Before T, all instances agree; after T, served flips to the new source.
   Rollback = append `FROM@<T2>` (future), never return to plain `FROM@0`.
   With groups, each group runs this step on its own switch.
4. When `cutover_routed{source=<old>, served="true"}` has been near zero for
   about a day, remove the old source and the processor, then clean up old
   offsets. Very late stragglers after this point are an accepted risk.

## 14. Guarantees and limits (README section)

Guarantees: each event served by one source; no offset resets, no coordination,
no shared state; deterministic cut; can be postponed before T or reversed after
it; bad input surfaces as message errors.

Limits: not end-to-end exactly-once (normal at-least-once still applies, the
handoff adds no new duplicate path); strict per-key order not guaranteed at T;
unsafe if instances run different switches past T; moving related event types
at different times can reorder them across types; dry run reads both sources;
producer clock skew only lengthens the late tail.

Don't use it when: no stable event time shared by both copies (use a dedupe
store); destination needs end-to-end exactly-once (use transactions); a short
pause is acceptable (drain, then switch); the data is state, not events
(snapshot plus CDC).

## 15. Coding guidelines

- Current stable Go; `gofmt`, `go vet`, `golangci-lint` clean.
- No globals, no panics in library code, wrapped errors with context.
- Pure logic in `internal/`; plugin glue thin.
- Doc comment on every exported identifier.
- Files small and focused (roughly under 300 lines).

## 16. Milestones

- **M1** `cutswitch` package + tests + property test.
- **M2a** `cutover` decision core + config validation + tests, simple form
  (one `switch`) only.
- **M2b** groups: `cutovers`, `event_type`, `unknown_event_type`.
- **M3** processor glue, metrics, startup logs, processor tests.
- **M4** custom binary, examples, docker-compose end-to-end.
- **M5** `cutover-switch` tool, README.

## 17. Out of scope

Exactly-once delivery, dedupe stores, runtime switch injection (Redis, APIs),
new top-level config sections.