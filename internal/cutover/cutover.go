// Package cutover decides, per message, whether the processor serves it.
package cutover

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/internal/cutswitch"
)

// Config is the simple form: one switch for every event.
type Config struct {
	Switch                string
	From, To, Passthrough []string
}

// Outcome is what Decide concluded for one message.
type Outcome int

// Outcomes of Decide.
const (
	Served        Outcome = iota // keep: its lane owns the ts
	NotServed                    // drop: the other lane owns the ts
	Passthrough                  // keep: source is not migrating
	UnknownSource                // keep and mark errored
	InvalidTS                    // keep and mark errored
)

// Router decides, per message, whether to serve it.
type Router struct {
	sw          cutswitch.Switch
	lanes       map[string]cutswitch.Lane // from/to sources
	passthrough map[string]bool
}

// New validates cfg and builds a Router. Any error must stop startup.
func New(cfg Config) (*Router, error) {
	sw, err := cutswitch.Parse(cfg.Switch)
	if err != nil {
		return nil, fmt.Errorf("switch: %w", err)
	}
	if len(cfg.From) == 0 || len(cfg.To) == 0 {
		return nil, errors.New("from and to must each list at least one source")
	}
	// A source in two lanes would make both of its copies kept or both dropped.
	seen := map[string]bool{}
	for _, list := range [][]string{cfg.From, cfg.To, cfg.Passthrough} {
		for _, src := range list {
			if seen[src] {
				return nil, fmt.Errorf("source %q is listed more than once", src)
			}
			seen[src] = true
		}
	}

	r := &Router{sw: sw, lanes: map[string]cutswitch.Lane{}, passthrough: map[string]bool{}}
	for _, src := range cfg.From {
		r.lanes[src] = cutswitch.From
	}
	for _, src := range cfg.To {
		r.lanes[src] = cutswitch.To
	}
	for _, src := range cfg.Passthrough {
		r.passthrough[src] = true
	}
	return r, nil
}

// Decide follows SPEC section 7: source first, then ts, then ownership.
func (r *Router) Decide(source string, ts any) (Outcome, error) {
	if r.passthrough[source] {
		return Passthrough, nil
	}
	lane, ok := r.lanes[source]
	if !ok {
		return UnknownSource, fmt.Errorf("source %q is not in from, to or passthrough", source)
	}
	ms, err := parseTimestamp(ts)
	if err != nil {
		return InvalidTS, err
	}
	if r.sw.Owner(ms) == lane {
		return Served, nil
	}
	return NotServed, nil
}

// parseTimestamp reads the value of the timestamp mapping as epoch ms.
// Integers are never routed through float64, so they stay exact.
func parseTimestamp(v any) (int64, error) {
	var ms int64
	switch val := v.(type) {
	case json.Number: // benthos decodes JSON numbers this way by default
		return parseTimestamp(string(val))
	case string:
		tm, err := time.Parse(time.RFC3339, val)
		if err == nil {
			ms = tm.UnixMilli()
			break
		}
		// The round trip rejects "+123" and leading zeros: only plain digits are accepted.
		if ms, err = strconv.ParseInt(val, 10, 64); err != nil || strconv.FormatInt(ms, 10) != val {
			return 0, fmt.Errorf("timestamp %q is neither epoch ms nor RFC 3339 with offset", val)
		}
	case int64:
		ms = val
	case uint64:
		if val > math.MaxInt64 {
			return 0, fmt.Errorf("timestamp %d overflows int64", val)
		}
		ms = int64(val)
	case float64:
		// Valid ts are far below 2^53, so a whole float64 holds them exactly.
		// Range is checked before converting: int64 of a huge float is undefined.
		if val != math.Trunc(val) || val < 1e12 || val >= 1e13 {
			return 0, fmt.Errorf("float timestamp %v must be a whole number in [1e12, 1e13)", val)
		}
		ms = int64(val)
	default:
		return 0, fmt.Errorf("timestamp has unsupported type %T", v)
	}
	// Catches negatives and seconds written instead of milliseconds.
	if ms < 1e12 || ms >= 1e13 {
		return 0, fmt.Errorf("timestamp %d is outside epoch ms range [1e12, 1e13)", ms)
	}
	return ms, nil
}
