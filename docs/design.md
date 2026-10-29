# Design

This is for people changing the code or reviewing it. If you just want to use
the processor, the [README](../README.md) is enough.

## The idea

Moving a consumer from one topic to another usually means stopping, draining,
resetting offsets, and hoping nothing slipped through. This processor avoids all
of that by reading both topics at once and letting each event's own timestamp
decide which copy is kept:

```
owner(ts) = the lane of the last cut whose time <= ts
keep the message if its lane == owner(ts)
```

Both copies of an event carry the same `ts`, so they reach the same answer, and
exactly one is kept. Lag, arrival order, rebalances and restarts don't matter,
and instances don't need to talk to each other. This only holds while the
[conditions in the README](../README.md#before-you-use-it) hold: same events,
same `ts`, same event type in both copies.

| Term | Meaning |
|---|---|
| source | Where a message came from, usually a topic name |
| lane | `from` (old sources), `to` (new sources) or `passthrough` (not moving) |
| ts | The event time in epoch milliseconds, set by the producer |
| cut | One entry of a switch: `FROM@<time>` or `TO@<time>` |
| switch | The ordered history of cuts, e.g. `TO@T1,FROM@T2` |
| group | Event types that share one switch |

## The switch

```
SWITCH := CUT ("," CUT)*
CUT    := ("FROM" | "TO") "@" TIME
TIME   := "0" | EPOCH_MS | RFC3339 | SIMPLE
SIMPLE := YYYY "-" MM "-" DD " " HH ":" MM " " ZONE
ZONE   := "UTC" | "GMT" | "Z" | ("+" | "-") HH ":" MM | IANA | "PT" | "MT" | "CT" | "ET"
```

Rules, each with the mistake it prevents:

- **`FROM@0` is implied at the start.** Every switch begins there, so writing
  it is optional. The dry run still writes `FROM@0`, because an empty switch is
  rejected: an unset environment variable expands to "", and that must stop
  startup, not quietly mean "no cutover".
- **Cuts alternate `FROM` and `TO`, and times strictly increase.** Otherwise
  `owner(ts)` would be ambiguous.
- **Rollback adds a cut; it never removes one.** With the full history, a late
  event before T1 still goes to the old topic and one in [T1, T2) still goes
  to the new one, so its copy is decided the same way it was before.
- **Every time carries its zone.** Pods usually run in UTC, so a time without a
  zone would quietly mean a different moment.
- **Zone abbreviations (`PST`, `EST`, `IST`) are rejected.** People write `PST`
  in summer, several abbreviations are ambiguous, and Go reads unknown ones as
  UTC without an error. IANA names and `PT`/`MT`/`CT`/`ET` apply daylight
  saving for the date instead. `Etc/GMT+8` is rejected for its inverted sign.
- **No 12-hour clock.** An am/pm slip moves the cut by 12 hours.
- **Times that don't exist or happen twice on a daylight saving change are
  rejected**, e.g. 02:30 on spring-forward. Go would silently pick one.
- **Epoch times are exactly 13 digits**, and every time except `0` must be in
  [1e12, 1e13), 2001-09-09 to 2286-11-20. This catches seconds written as
  milliseconds; a cut outside the range could never match a valid event.
- **A cut in the past is fine at startup.** Pods restart after the cut all the
  time and must keep working. Checking that a new cut is far enough ahead is a
  deploy-time job, done by `cutover-switch check`.

The zone database is embedded (`time/tzdata`, about 450 KB) so IANA names work
in minimal containers.

## Event timestamps

The `timestamp` mapping may return:

- a JSON number or a plain digit string: epoch milliseconds
- an RFC 3339 string with an offset: converted to epoch milliseconds

Rejected, marking the message as errored with reason `invalid_ts`: missing or
null values, fractions, negatives, strings with signs or leading zeros, values
outside [1e12, 1e13), and `uint64` values that overflow `int64`. Integers are
never converted through `float64`, so they stay exact.

## Deciding one message

1. Read `source`. If it's in `passthrough`, keep the message unchanged and stop.
   If it's in no lane, mark it errored (`unknown_source`) and stop.
2. Read `timestamp`. If it's invalid, mark it errored (`invalid_ts`) and stop.
3. Find the group: by `event_type` when configured, falling back to `"*"`. If
   none matches, mark it errored (`unknown_event_type`) and stop.
4. Keep the message if its lane owns the `ts`; otherwise drop it.
5. Record the metrics.

Errored messages are kept with Redpanda Connect's usual error flag, so the
standard error handling (`errored()`, a DLQ output) applies. Bad input is never
dropped silently.

## Startup validation

Any of these stops the processor from starting:

- both `switch` and `cutovers` set, or neither
- `from` or `to` empty, or a source listed in more than one lane
- an empty or duplicated group name, or a group with no events
- an event type in more than one group
- event types listed without an `event_type` mapping
- any invalid switch

The simple form is one group named `default` with `events: ["*"]`. Its errors
and log line refer to the switch you wrote, never to that group.

## Code layout

```
internal/cutover/              standard library only
  switch.go                    parse a switch, Owner(ts)
  router.go                    validate the config, Decide(source, ts, eventType)
processor/                     Redpanda Connect glue: config spec, Process, metrics, logs
cmd/redpanda-connect-cutover/  Redpanda Connect's open-source build plus this processor
cmd/cutover-switch/            explain and check a switch
examples/                      simple, groups and DLQ pipelines
deploy/                        local Redpanda and the end-to-end script
```

`processor` is the only package that imports Redpanda Connect, and it uses only
the public `service` API.

### Moving it upstream

The code is shaped so Redpanda Connect could adopt it by copying, not
importing:

- `internal/cutover` has no dependencies beyond the standard library, so it
  copies over with only its import path changed.
- `processor.Register(env)` registers the processor explicitly instead of in
  `init()`. Upstream would call it from its usual `init()`.
- User docs live in the config spec, because upstream generates its docs from
  it. The processor is marked beta.
- Two pieces can be dropped independently if reviewers object: the embedded
  zone database and the US zone aliases.
- Upstream also needs `gofumpt`, license headers, generated docs and an
  `info.csv` entry, and asks for an issue to agree on scope first.

## Testing

- **Switch parsing:** every rule above, with winter and summer dates, DST gaps
  and overlaps, rejected formats, and range boundaries.
- **Ownership:** boundaries at T and T-1, rollback history, and a randomized
  test that the owner flips exactly at each cut and nowhere else.
- **Decisions:** every outcome, and a test that exactly one copy per group is
  served.
- **Processor:** config errors, every outcome through Redpanda Connect, the
  startup log, and metric values through the Prometheus exporter.
- **Fuzzing:** `FuzzParseSwitch` and `FuzzParseTimestamp` check that no input
  panics and nothing invalid is accepted. Run longer with `go test -fuzz`.
- **End to end:** `deploy/e2e.sh` runs a dry run, a cut and a rollback against
  a real Redpanda and checks that each event comes out once, from the right
  topic.

## Out of scope

Exactly-once delivery, dedupe stores, changing the switch at runtime (through
Redis or an API, say), and new top-level config sections.
