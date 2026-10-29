# event_time_cutover

A Redpanda Connect processor that moves a running consumer from an old topic to
a new one carrying the same events, with nothing lost and nothing read twice.
No offset resets, no coordination between instances, no shared state.

This is an unofficial community plugin for Redpanda Connect.
Built with AI assistance (Claude). Every change was reviewed, tested and
approved by the author.

## How it works

Your producer writes every event to both topics for a while. The pipeline reads
both. Each event has an event time (`ts`) set by the producer, and a **switch**
says which topic owns which range of `ts`:

```
TO@2026-11-15 14:30 PT
```

Events before 14:30 PT are served from the old topic; events from 14:30 PT on
are served from the new one. The other copy is dropped. Because the decision
depends only on the event's own timestamp, every instance makes the same choice
without talking to the others, and restarts, lag and rebalances don't change it.

The exact rules, and the reasons behind them, are in
[docs/design.md](docs/design.md).

## Before you use it

These must hold, or events can be lost or duplicated:

1. Both topics carry the same events.
2. Each event has the **same `ts` in both copies**: the time the producer
   created it, never the broker's arrival time.
3. If you use groups, each event has the same event type in both copies.
4. Processors placed before this one treat both copies the same way (see
   [Placement](#placement)).

## Quick start

Requires Go 1.27.3 or later.

```sh
go build ./cmd/redpanda-connect-cutover
CUTOVER_SWITCH=FROM@0 ./redpanda-connect-cutover run examples/simple.yaml
```

`redpanda-connect-cutover` is Redpanda Connect with this processor added. It
includes all of Redpanda Connect's open-source connectors, so your existing
pipeline runs on it unchanged.

To see a full cutover and rollback on a local Redpanda (needs Docker and `jq`):

```sh
./deploy/e2e.sh
```

## Config

One switch for all events:

```yaml
pipeline:
  processors:
    - event_time_cutover:
        switch: ${CUTOVER_SWITCH}
        from: [orders]
        to: [orders_v2]
        timestamp: this.ts
```

Different event types cutting over at different times:

```yaml
    - event_time_cutover:
        cutovers:
          - name: orders
            events: [order_created, order_cancelled]
            switch: ${ORDERS_SWITCH}
          - name: rest
            events: ["*"]
            switch: ${REST_SWITCH}
        from: [orders]
        to: [orders_v2]
        passthrough: [payments]
        timestamp: this.ts
        event_type: this.type
```

| Field | Meaning |
|---|---|
| `switch` | The switch for all events. Use this or `cutovers`, not both. |
| `cutovers` | Groups of event types, each with its own `name`, `events` and `switch`. `"*"` matches every event type no other group lists. |
| `from` | Old topics. |
| `to` | New topics carrying the same events. |
| `passthrough` | Topics that aren't moving. Their messages are kept untouched. |
| `source` | Where a message came from. Default: `meta("kafka_topic")`. |
| `timestamp` | The event time: epoch milliseconds or RFC 3339 with an offset. |
| `event_type` | The event type. Required when a group lists event types. |

Full examples are in [`examples/`](examples).

## The switch

A switch is a list of cuts. Each cut hands ownership to a topic from a moment
on:

| Switch | Meaning |
|---|---|
| `FROM@0` | The old topic owns everything. This is the dry run. |
| `TO@T1` | New owns events from T1 on; old owns everything before. |
| `TO@T1,FROM@T2` | Rollback: new owns events from T1 up to T2; old owns the rest. |

To roll back, add a cut; never delete one. Keeping the history means a late
event from before T1 still goes to the old topic, and a late event between T1
and T2 still goes to the new one, so nothing is served twice.

Times can be written as:

| Form | Example |
|---|---|
| Date, time and zone (preferred) | `2026-11-15 14:30 PT` |
| IANA zone name | `2026-11-15 14:30 America/Los_Angeles` |
| UTC or an offset | `2026-11-15 22:30 UTC`, `2026-11-15 14:30 -08:00` |
| RFC 3339 | `2026-11-15T22:30:00Z` |
| Epoch milliseconds | `1794781800000` |

`PT`, `MT`, `CT` and `ET` are the US zones, with daylight saving applied for
the date you write. Abbreviations like `PST` or `EST` are rejected, because
people often use the wrong one for the season. A 12-hour clock is rejected too,
and so is a time that doesn't exist or happens twice when the clocks change.

A bad switch stops the processor from starting. An empty one does too, so an
unset environment variable can't silently mean "no cutover".

To see what a switch means before you deploy it:

```sh
$ go run ./cmd/cutover-switch explain "TO@2026-11-15 14:30 PT,FROM@2026-11-16 09:00 PT"
TO@1794781800000    2026-11-15T22:30:00Z  2026-11-15 14:30 PST
FROM@1794848400000  2026-11-16T17:00:00Z  2026-11-16 09:00 PST

ts < 2026-11-15T22:30:00Z                          FROM
2026-11-15T22:30:00Z <= ts < 2026-11-16T17:00:00Z  TO
ts >= 2026-11-16T17:00:00Z                         FROM
```

## Rollout

1. **Deploy with `FROM@0`, reading the old topic only.** List the new topic
   in `to` already; the input just doesn't read it yet. Nothing changes.
   Check that `cutover_cut_info` shows one series per group.
2. **Dry run: add the new topic to the input.** The new topic has no
   committed offsets yet, so it starts from the input's start offset. Use
   earliest, or a time safely before your cut, never latest: with latest,
   events written to it before the restart would be skipped. Compare the
   counts per event type (see [Metrics](#metrics)) and check CPU and memory.
3. **Cut over: set `TO@<T>`,** one environment at a time. Pick T far enough
   ahead that every instance runs the new switch before T arrives. Run
   `cutover-switch check "$CUTOVER_SWITCH" --min-lead 30m` in CI to enforce
   that. With groups, each group goes through this step on its own.
4. **Roll back if needed** by adding `FROM@<T2>`, with T2 in the future too.
   Never go back to plain `FROM@0`.
5. **Clean up.** Once `cutover_routed{source="<old>",served="true"}` has been
   near zero for about a day, remove the old topic and the processor, then
   delete the old consumer offsets. Events that arrive later than that are
   an accepted risk.

Startup doesn't reject a cut in the past: pods restart after the cut all the
time, and they must keep working. Only a deploy needs the future check, which
is why it lives in `cutover-switch check`.

## Placement

The processor can go anywhere in the pipeline. What matters is what runs
before it.

Safe before it: filters on the event's content, decoding (JSON, Avro,
Protobuf), and extracting the timestamp.

Not safe before it: anything that depends on the topic, partition, offset or
arrival time; anything that changes the timestamp or event type; stateful
processors like `dedupe` or caches that might drop only one copy; and anything
that merges or splits messages.

## Errors and DLQ

Messages the processor can't place are kept and marked as errored, never
dropped silently:

| Reason | When |
|---|---|
| `unknown_source` | The source is not in `from`, `to` or `passthrough`. |
| `invalid_ts` | The timestamp is missing, isn't epoch milliseconds or RFC 3339, or is out of range. |
| `unknown_event_type` | No group matches the event type and there is no `"*"` group. |

Route them like any other error in Redpanda Connect. For example, send them to
a dead-letter topic with an output `switch` on `errored()`; see
[`examples/dlq.yaml`](examples/dlq.yaml).

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `cutover_routed` | `group`, `source`, `event_type`, `served` | Messages decided. `served="false"` copies were dropped. |
| `cutover_errors` | `reason` | Messages marked as errored. |
| `cutover_cut_info` | `group`, `cut_ts`, `owner` | Set to 1 at startup for the newest cut. |

During the dry run, for each event type, the old topic's `served="true"` count
should match the new topic's `served="false"` count. If they match, the new
topic really carries the same events.

All instances should report the same `cut_info` series per group. Two different
series mean instances run different switches. After a rollout, the old series
can linger briefly.

Keep `event_type` values few: each one becomes its own metric series.

The processor logs one line per group at startup, with every cut and the
current owner:

```
event_time_cutover: one switch for all events: cuts=[FROM@0, TO@1794781800000 (2026-11-15T22:30:00Z)] owner now=from
```

## Guarantees and limits

It guarantees that each event is served from exactly one topic, with no offset
resets and no coordination. The cut is deterministic, can be postponed before T
and reversed after it, and bad input shows up as message errors.

It does not give you:

- **End-to-end exactly-once.** Normal at-least-once delivery still applies;
  the handoff adds no new way to duplicate.
- **Strict order per key at T.** Events right around the cut can come from
  different topics.
- **Safety with mixed switches.** Instances running different switches past T
  can lose or duplicate events.
- **Order across event types** moved at different times.

The dry run reads both topics, so expect extra load. Producer clock skew only
makes the late tail longer.

Don't use it when:

- the two copies don't share a stable event time (use a dedupe store),
- the destination needs end-to-end exactly-once (use transactions),
- a short pause is fine (drain the old topic, then switch),
- the data is state, not events (use a snapshot plus CDC).

## Use it in your own binary

`redpanda-connect-cutover` is about 400 MB because it includes every
open-source connector. To build a slimmer one with only the connectors you
need, register the processor yourself. This one, with Kafka/Redpanda and the
core processors, is about 45 MB:

```go
import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/processor"
)

func main() {
	if err := processor.Register(service.GlobalEnvironment()); err != nil {
		panic(err)
	}
	service.RunCLI(context.Background())
}
```

## License

Apache 2.0. See [LICENSE](LICENSE).
