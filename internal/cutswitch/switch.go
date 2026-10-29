// Package cutswitch parses a cutover switch and decides which lane owns an event time.
package cutswitch

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // embed zone data so IANA names work in minimal containers
)

// Lane identifies a side of the cutover.
type Lane int

// Lanes a message can be served from.
const (
	From Lane = iota // old source
	To               // new source
)

// Cut moves ownership to Lane for every event time >= At (epoch ms).
type Cut struct {
	Lane Lane
	At   int64
}

// Switch is an ordered history of cuts. The first cut is always FROM@0.
// The zero Switch behaves like FROM@0.
type Switch struct {
	cuts []Cut
}

// Parse reads a switch like "TO@2026-11-15 14:30 PT". FROM@0 at the start is
// implied, so "FROM@0,TO@..." means the same.
func Parse(s string) (Switch, error) {
	// An unset env var expands to "", which must stop startup, not mean "no cutover".
	if strings.TrimSpace(s) == "" {
		return Switch{}, errors.New("parse switch: empty")
	}
	cuts := []Cut{{Lane: From, At: 0}}
	for i, raw := range strings.Split(s, ",") {
		raw = strings.TrimSpace(raw)
		cut, err := parseCut(raw)
		if err != nil {
			return Switch{}, fmt.Errorf("parse cut %q: %w", raw, err)
		}
		if i == 0 && cut == cuts[0] {
			continue // the implied FROM@0, written out
		}
		if err := checkOrder(cuts, cut); err != nil {
			return Switch{}, fmt.Errorf("parse cut %q: %w", raw, err)
		}
		cuts = append(cuts, cut)
	}
	return Switch{cuts: cuts}, nil
}

func parseCut(raw string) (Cut, error) {
	if raw == "" {
		return Cut{}, errors.New("empty cut, check for a trailing or double comma")
	}
	keyword, at, ok := strings.Cut(raw, "@")
	if !ok {
		return Cut{}, errors.New("want FROM@<time> or TO@<time>")
	}
	var lane Lane
	switch keyword {
	case "FROM":
		lane = From
	case "TO":
		lane = To
	default:
		return Cut{}, fmt.Errorf("unknown lane %q, want FROM or TO", keyword)
	}
	if at == "0" {
		return Cut{Lane: lane, At: 0}, nil
	}
	ms, err := parseTime(at)
	if err != nil {
		return Cut{}, err
	}
	// Event timestamps must fall in this range too, so a cut outside it could never match an event.
	if ms < 1e12 || ms >= 1e13 {
		return Cut{}, fmt.Errorf("time %q is outside 2001-09-09 .. 2286-11-20", at)
	}
	return Cut{Lane: lane, At: ms}, nil
}

// checkOrder rejects histories that Owner could not read unambiguously.
func checkOrder(cuts []Cut, next Cut) error {
	last := cuts[len(cuts)-1]
	if next.Lane == last.Lane {
		return errors.New("cuts must alternate FROM and TO")
	}
	if next.At <= last.At {
		return errors.New("cut times must strictly increase")
	}
	return nil
}

// parseTime returns epoch ms. Every accepted form carries its zone, so the
// result never depends on the time zone of the pod.
func parseTime(s string) (int64, error) {
	if s != "" && strings.Trim(s, "0123456789") == "" {
		// 13 digits catches times written in seconds instead of milliseconds.
		if len(s) != 13 {
			return 0, fmt.Errorf("epoch time %q must be 13 digits of milliseconds", s)
		}
		return strconv.ParseInt(s, 10, 64)
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli(), nil
	}
	if t, err := time.Parse("2006-01-02 15:04 Z07:00", s); err == nil {
		return t.UnixMilli(), nil
	}
	return parseWallClock(s)
}

// parseWallClock reads "2026-11-15 14:30 PT": a clock time in a named zone,
// with daylight saving applied for that date.
func parseWallClock(s string) (int64, error) {
	parts := strings.Split(s, " ")
	if len(parts) != 3 {
		return 0, fmt.Errorf(`unknown time %q, want 0, 13-digit epoch ms, RFC 3339 or "YYYY-MM-DD HH:MM ZONE"`, s)
	}
	loc, err := location(parts[2])
	if err != nil {
		return 0, err
	}
	const layout = "2006-01-02 15:04"
	wall, err := time.Parse(layout, parts[0]+" "+parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid time %q: %w", s, err)
	}
	t := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), 0, 0, loc)

	// Go silently moves times that fall in a daylight saving change; we refuse them.
	written := wall.Format(layout)
	if t.Format(layout) != written {
		return 0, fmt.Errorf("time %q does not exist: clocks skip it for daylight saving", s)
	}
	_, before := t.Add(-24 * time.Hour).Zone()
	_, after := t.Add(24 * time.Hour).Zone()
	dstShift := time.Duration(after-before) * time.Second
	earlier, later := t.Add(-dstShift).Format(layout), t.Add(dstShift).Format(layout)
	if dstShift != 0 && (earlier == written || later == written) {
		return 0, fmt.Errorf("time %q occurs twice when daylight saving ends", s)
	}
	return t.UnixMilli(), nil
}

// location refuses abbreviations like EST or PST: they ignore daylight saving
// and are often used for the wrong season. Etc/GMT+8 is refused for its
// inverted sign (it means -08:00).
func location(zone string) (*time.Location, error) {
	switch zone {
	case "UTC", "GMT":
		return time.UTC, nil
	case "PT":
		zone = "America/Los_Angeles"
	case "MT":
		zone = "America/Denver"
	case "CT":
		zone = "America/Chicago"
	case "ET":
		zone = "America/New_York"
	}
	if !strings.Contains(zone, "/") || strings.HasPrefix(zone, "Etc/") {
		return nil, fmt.Errorf("unknown zone %q, want UTC, GMT, PT, MT, CT, ET or an IANA name", zone)
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return nil, fmt.Errorf("load zone %q: %w", zone, err)
	}
	return loc, nil
}

// Owner returns the lane that serves events with the given event time.
func (s Switch) Owner(ts int64) Lane {
	// A switch holds a handful of cuts, so a linear scan is clearest. Before
	// any cut the old source owns everything, matching the FROM@0 start.
	owner := From
	for _, c := range s.cuts {
		if c.At > ts {
			break
		}
		owner = c.Lane
	}
	return owner
}

// Cuts returns a copy of the cut history, oldest first.
func (s Switch) Cuts() []Cut {
	return slices.Clone(s.cuts)
}

// String returns "from" or "to", as used in logs and metric labels.
func (l Lane) String() string {
	if l == To {
		return "to"
	}
	return "from"
}
