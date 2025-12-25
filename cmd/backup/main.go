package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/davexpro/backup/internal/gitlab"
	"github.com/davexpro/backup/internal/mysql"
	"github.com/davexpro/backup/internal/setup"
)

var (
	date      = "not provided (use build.sh instead of 'go build')"
	magic     = "not provided (use build.sh instead of 'go build')"
	startTime = time.Now()
)

func printVersion() {
	fmt.Printf("%10s : %s\n", "built", runtime.Version())
	fmt.Printf("%10s : %s\n", "date", date)
	fmt.Printf("%10s : %s\n", "magic", magic)
}

func main() {
	printVersion()

	cmd := &cli.Command{
		Name:  "backup",
		Usage: "MySQL backup tool with separate compression, SHA256, MinIO upload, retention policy and history logging",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "config.yaml",
				Usage:   "Load configuration from `FILE`",
			},
			&cli.BoolFlag{
				Name:  "only-dump",
				Usage: "Only backup data to local directory, do not upload to cloud",
			},
		},
		Commands: []*cli.Command{
			setup.Command,
			mysql.Command,
			gitlab.Command,
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
