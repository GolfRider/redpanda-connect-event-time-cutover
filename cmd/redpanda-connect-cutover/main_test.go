package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/processor"
)

func TestExamplesLint(t *testing.T) {
	t.Setenv("CUTOVER_SWITCH", "TO@2026-11-15 14:30 PT")
	t.Setenv("ORDERS_SWITCH", "TO@2026-11-15 14:30 PT")
	t.Setenv("REST_SWITCH", "FROM@0")
	env := service.NewEnvironment()
	if err := processor.Register(env); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("../../examples/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	args := append([]string{"redpanda-connect-cutover", "lint"}, files...)
	code, err := service.RunCLIToCode(context.Background(),
		service.CLIOptSetEnvironment(env), service.CLIOptSetArgs(args...))
	if err != nil || code != 0 {
		t.Fatalf("lint exit code %d, error %v", code, err)
	}
}
