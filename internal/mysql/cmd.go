package mysql

import (
	"context"
	"fmt"
	"log"

	"github.com/urfave/cli/v3"

	"github.com/davexpro/backup/internal/config"
	"github.com/davexpro/backup/internal/pkg/helper"
)

var Command = &cli.Command{
	Name:  "mysql",
	Usage: "MySQL backup and recovery operations",
	Commands: []*cli.Command{
		{
			Name:  "dump",
			Usage: "Execute the MySQL backup workflow",
			Action: func(ctx context.Context, c *cli.Command) error {
				cfg, store, notifier, unlock, err := prepare(c)
				if err != nil {
					return err
				}
				defer unlock()

				log.Printf("Starting MySQL backup (dump) workflow")
				worker := NewWorker(cfg, store, notifier, c.Bool("only-dump"))
				return worker.Backup(ctx)
			},
		},
		{
			Name:  "recover",
			Usage: "Restore data from a backup path",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "input",
					Aliases:  []string{"i"},
					Usage:    "Path to the dump directory or zip file",
					Required: true,
				},
			},
			Action: func(ctx context.Context, c *cli.Command) error {
				cfg, store, notifier, unlock, err := prepare(c)
				if err != nil {
					return err
				}
				defer unlock()

				inputPath := c.String("input")
				log.Printf("Starting MySQL recovery from: %s", inputPath)
				worker := NewWorker(cfg, store, notifier, c.Bool("only-dump"))
				return worker.Recover(ctx, inputPath)
			},
		},
	},
}

func prepare(c *cli.Command) (*config.Config, *helper.Storage, *helper.TelegramSender, func(), error) {
	// 1. Check required tools
	if err := helper.CheckTools("mysqlsh", "zip", "unzip"); err != nil {
		return nil, nil, nil, nil, err
	}

	// 2. Load config
	configPath := c.String("config")
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 2. File locking
	unlock, err := helper.AcquireLock(cfg.LockFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("could not acquire lock: %w", err)
	}

	// 3. Initialize Telegram notifier
	notifier := helper.NewTelegramSender(cfg.Telegram.BotToken, cfg.Telegram.ChatID)

	// 4. Initialize storage
	store, err := helper.NewStorage(cfg.R2)
	if err != nil {
		unlock()
		return nil, nil, nil, nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return cfg, store, notifier, unlock, nil
}
