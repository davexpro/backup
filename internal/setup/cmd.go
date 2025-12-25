package setup

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"
)

var Command = &cli.Command{
	Name:   "setup",
	Usage:  "Install dependencies (mysqlsh) on Debian-based systems",
	Action: run,
}

func run(ctx context.Context, c *cli.Command) error {
	// 1. OS Check (Debian only)
	if !isDebian() {
		return fmt.Errorf("the setup command is only supported on Debian-based systems")
	}

	return checkAndInstallForDebian()
}
