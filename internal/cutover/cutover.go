// Package cutover decides, per message, whether the processor serves it.
package cutover

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/internal/cutswitch"
)

// Config is the processor config: either Switch (simple form) or Groups.
type Config struct {
	Switch                string
	Groups                []Group
	From, To, Passthrough []string
	HasEventType          bool // the event_type mapping is configured
}

// Group is one cutovers entry: event types that share a switch.
type Group struct {
	Name   string
	Events []string // "*" matches event types no other group lists
	Switch string
}

// Outcome is what Decide concluded for one message.
type Outcome int

// Outcomes of Decide.
const (
	Served           Outcome = iota // keep: its lane owns the ts
	NotServed                       // drop: the other lane owns the ts
	Passthrough                     // keep: source is not migrating
	UnknownSource                   // keep and mark errored
	InvalidTS                       // keep and mark errored
	UnknownEventType                // keep and mark errored
)

// GroupInfo describes one group for the startup log and the cut_info metric.
type GroupInfo struct {
	Name   string
	Events []string
	Cuts   []cutswitch.Cut
}

// Router decides, per message, whether to serve it.
type Router struct {
	groups      map[string]group          // event type -> group; "*" is the catch-all
	infos       []GroupInfo               // in config order
	lanes       map[string]cutswitch.Lane // from/to sources
	passthrough map[string]bool
}

type group struct {
	name string
	sw   cutswitch.Switch
}

// New validates cfg and builds a Router. Any error must stop startup.
func New(cfg Config) (*Router, error) {
	if (cfg.Switch == "") == (len(cfg.Groups) == 0) {
		return nil, errors.New("set exactly one of switch or cutovers")
	}
	if cfg.Switch != "" {
		// Report errors against the switch the user wrote, not the implicit group.
		if _, err := cutswitch.Parse(cfg.Switch); err != nil {
			return nil, fmt.Errorf("switch: %w", err)
		}
		cfg.Groups = []Group{{Name: "default", Events: []string{"*"}, Switch: cfg.Switch}}
	}
	groups, infos, err := newGroups(cfg.Groups)
	if err != nil {
		return nil, err
	}
	// Without an event_type mapping every message looks the same, so only a lone "*" group works.
	if _, star := groups["*"]; !cfg.HasEventType && (len(groups) != 1 || !star) {
		return nil, errors.New("event_type must be set when a group lists event types")
	}
	r := &Router{groups: groups, infos: infos}
	if err := r.setSources(cfg.From, cfg.To, cfg.Passthrough); err != nil {
		return nil, err
	}
	return r, nil
}

// setSources validates the source lists and maps each source to its lane.
func (r *Router) setSources(from, to, passthrough []string) error {
	if len(from) == 0 || len(to) == 0 {
		return errors.New("from and to must each list at least one source")
	}
	// A source in two lanes would make both of its copies kept or both dropped.
	seen := map[string]bool{}
	for _, list := range [][]string{from, to, passthrough} {
		for _, src := range list {
			if seen[src] {
				return fmt.Errorf("source %q is listed more than once", src)
			}
			seen[src] = true
		}
	}

	r.lanes = map[string]cutswitch.Lane{}
	r.passthrough = map[string]bool{}
	for _, src := range from {
		r.lanes[src] = cutswitch.From
	}
	for _, src := range to {
		r.lanes[src] = cutswitch.To
	}
	for _, src := range passthrough {
		r.passthrough[src] = true
	}
	return nil
}

// newGroups validates the groups, indexes them by event type and describes them.
func newGroups(groups []Group) (map[string]group, []GroupInfo, error) {
	byEvent := map[string]group{}
	names := map[string]bool{}
	var infos []GroupInfo
	for _, g := range groups {
		if g.Name == "" || names[g.Name] {
			return nil, nil, fmt.Errorf("group %q: name is empty or duplicated", g.Name)
		}
		names[g.Name] = true
		if len(g.Events) == 0 {
			return nil, nil, fmt.Errorf("group %q has no events", g.Name)
		}
		sw, err := cutswitch.Parse(g.Switch)
		if err != nil {
			return nil, nil, fmt.Errorf("group %q: switch: %w", g.Name, err)
		}
		for _, ev := range g.Events {
			if _, dup := byEvent[ev]; dup {
				return nil, nil, fmt.Errorf("event type %q is in more than one group", ev)
			}
			byEvent[ev] = group{name: g.Name, sw: sw}
		}
		infos = append(infos, GroupInfo{Name: g.Name, Events: g.Events, Cuts: sw.Cuts()})
	}
	return byEvent, infos, nil
}

// Groups describes every group, in config order.
func (r *Router) Groups() []GroupInfo {
	return slices.Clone(r.infos)
}

// Decide checks the source, then the ts, then the event type, then ownership.
// It returns the outcome, the group name (empty when no group applies) and,
// for the errored outcomes, the reason.
func (r *Router) Decide(source string, ts any, eventType string) (Outcome, string, error) {
	if r.passthrough[source] {
		return Passthrough, "", nil
	}
	lane, ok := r.lanes[source]
	if !ok {
		return UnknownSource, "", fmt.Errorf("source %q is not in from, to or passthrough", source)
	}
	ms, err := parseTimestamp(ts)
	if err != nil {
		return InvalidTS, "", err
	}
	g, ok := r.groups[eventType]
	if !ok {
		g, ok = r.groups["*"]
	}
	if !ok {
		return UnknownEventType, "", fmt.Errorf("event type %q matches no group", eventType)
	}
	if g.sw.Owner(ms) == lane {
		return Served, g.name, nil
	}
	return NotServed, g.name, nil
}

// parseTimestamp reads the value of the timestamp mapping as epoch ms.
// Integers are never routed through float64, so they stay exact.
func parseTimestamp(v any) (int64, error) {
	var ms int64
	switch val := v.(type) {
	case json.Number: // benthos decodes JSON numbers this way by default
		return parseTimestamp(string(val))
	case string:
		// The round trip rejects "+123" and leading zeros: only plain digits are accepted.
		n, err := strconv.ParseInt(val, 10, 64)
		if err == nil && strconv.FormatInt(n, 10) == val {
			ms = n
		} else if tm, err := time.Parse(time.RFC3339, val); err == nil {
			ms = tm.UnixMilli()
		} else {
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
