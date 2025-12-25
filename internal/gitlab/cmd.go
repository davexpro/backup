package gitlab

import (
	"context"
	"fmt"
	"log"

	"github.com/urfave/cli/v3"

	"github.com/davexpro/backup/internal/config"
	"github.com/davexpro/backup/internal/pkg/helper"
)

var Command = &cli.Command{
	Name:   "gitlab",
	Usage:  "Run GitLab backup workflow (Docker-based)",
	Action: run,
}

func run(ctx context.Context, c *cli.Command) error {
	// 1. Check required tools
	if err := helper.CheckTools("docker", "zip", "unzip"); err != nil {
		return err
	}

	// 2. Load config
	configPath := c.String("config")
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// 3. File locking
	unlock, err := helper.AcquireLock(cfg.LockFile)
	if err != nil {
		return fmt.Errorf("could not acquire lock: %w", err)
	}
	defer unlock()

	log.Printf("Starting GitLab backup workflow using config: %s", configPath)
	log.Printf("GitLab container: %s", cfg.GitLab.ContainerName)

	// 3. Initialize Telegram notifier
	notifier := helper.NewTelegramSender(cfg.Telegram.BotToken, cfg.Telegram.ChatID)

	// 4. Initialize storage
	store, err := helper.NewStorage(cfg.R2)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	// 5. Create and run worker
	worker := NewWorker(cfg, store, notifier, c.Bool("only-dump"))
	if err := worker.Run(ctx); err != nil {
		return err
	}

	log.Println("GitLab backup workflow completed successfully.")
	return nil
}
