package setup

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var Command = &cli.Command{
	Name:   "setup",
	Usage:  "Install dependencies (mysqlsh) on Debian-based systems",
	Action: run,
}

func run(c *cli.Context) error {
	// 1. OS Check (Debian only)
	if !isDebian() {
		return fmt.Errorf("the setup command is only supported on Debian-based systems")
	}

	return checkAndInstallMySQLShell()
}
