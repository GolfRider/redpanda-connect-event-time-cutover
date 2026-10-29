// Command cutover-switch explains a cutover switch and checks it before a deploy.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/internal/cutswitch"
)

const usage = `usage:
  cutover-switch explain "<switch>"
  cutover-switch check "<switch>" [--min-lead 30m]
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now()))
}

// run returns the exit code: 0 ok, 1 invalid switch or failed check, 2 bad usage.
// Local times are shown in now's location, so tests can fix both.
func run(args []string, stdout, stderr io.Writer, now time.Time) int {
	if len(args) == 1 && args[0] == "help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if len(args) < 2 || (args[0] != "explain" && args[0] != "check") || (args[0] == "explain" && len(args) > 2) {
		fmt.Fprint(stderr, usage)
		return 2
	}
	sw, err := cutswitch.Parse(args[1])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if args[0] == "explain" {
		explain(stdout, sw.Cuts(), now.Location())
		return 0
	}
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	minLead := flags.Duration("min-lead", 0, "least time from now to the newest cut")
	if err := flags.Parse(args[2:]); err != nil {
		return 2
	}
	return check(stdout, stderr, sw.Cuts(), now, *minLead)
}

// explain prints each cut, then which lane owns each range of event time.
func explain(out io.Writer, cuts []cutswitch.Cut, local *time.Location) {
	if len(cuts) == 1 {
		fmt.Fprintln(out, "dry run: FROM owns every ts")
		return
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, c := range cuts[1:] {
		t := time.UnixMilli(c.At)
		fmt.Fprintf(w, "%s\t%s\t%s\n", label(c), t.UTC().Format(time.RFC3339), t.In(local).Format("2006-01-02 15:04 MST"))
	}
	fmt.Fprintln(w)
	for i, c := range cuts {
		owner := strings.ToUpper(c.Lane.String())
		switch i {
		case 0:
			fmt.Fprintf(w, "ts < %s\t%s\n", utc(cuts[1].At), owner)
		case len(cuts) - 1:
			fmt.Fprintf(w, "ts >= %s\t%s\n", utc(c.At), owner)
		default:
			fmt.Fprintf(w, "%s <= ts < %s\t%s\n", utc(c.At), utc(cuts[i+1].At), owner)
		}
	}
	_ = w.Flush()
}

// check fails when the newest cut is too close: if the rollout is still going
// at the cut, instances on the old switch disagree with those on the new one.
func check(stdout, stderr io.Writer, cuts []cutswitch.Cut, now time.Time, minLead time.Duration) int {
	newest := cuts[len(cuts)-1]
	if newest.At == 0 {
		fmt.Fprintln(stdout, "ok: dry run, no cut to check")
		return 0
	}
	lead := time.UnixMilli(newest.At).Sub(now).Round(time.Second)
	if lead < 0 {
		fmt.Fprintf(stderr, "fail: newest cut %s (%s) is %s in the past\n", label(newest), utc(newest.At), -lead)
		return 1
	}
	if lead < minLead {
		fmt.Fprintf(stderr, "fail: newest cut %s (%s) is in %s, want at least %s\n", label(newest), utc(newest.At), lead, minLead)
		return 1
	}
	fmt.Fprintf(stdout, "ok: newest cut %s (%s) is in %s\n", label(newest), utc(newest.At), lead)
	return 0
}

func label(c cutswitch.Cut) string {
	return fmt.Sprintf("%s@%d", strings.ToUpper(c.Lane.String()), c.At)
}

func utc(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
