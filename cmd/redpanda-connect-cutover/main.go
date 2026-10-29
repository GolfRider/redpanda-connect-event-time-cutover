// Command redpanda-connect-cutover is Redpanda Connect with the
// event_time_cutover processor added. It runs any existing pipeline unchanged.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	"github.com/GolfRider/redpanda-connect-event-time-cutover/processor"
)

func main() {
	if err := processor.Register(service.GlobalEnvironment()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	service.RunCLI(context.Background(),
		service.CLIOptSetBinaryName("redpanda-connect-cutover"),
		service.CLIOptSetProductName("Redpanda Connect"),
	)
}
