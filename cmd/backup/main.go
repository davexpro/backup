package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/davexpro/backup/internal/backup"
	"github.com/davexpro/backup/internal/config"
	model "github.com/davexpro/backup/internal/db"
	"github.com/davexpro/backup/internal/notify"
	"github.com/davexpro/backup/internal/storage"
	"github.com/davexpro/backup/internal/utils"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	app := &cli.App{
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
			{
				Name:  "setup",
				Usage: "Install dependencies (mysqlsh) on Debian-based systems",
				Action: func(c *cli.Context) error {
					return backup.CheckAndInstallMySQLShell()
				},
			},
			{
				Name:  "mysql",
				Usage: "Run MySQL backup workflow",
				Action: func(c *cli.Context) error {
					return runWorkflow(c, func(ctx context.Context, m *backup.Manager) error {
						return m.Run(ctx)
					})
				},
			},
			{
				Name:  "gitlab",
				Usage: "Run GitLab backup workflow (Docker-based)",
				Action: func(c *cli.Context) error {
					return runWorkflow(c, func(ctx context.Context, m *backup.Manager) error {
						return m.BackupGitLab(ctx)
					})
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runWorkflow(c *cli.Context, workflow func(context.Context, *backup.Manager) error) error {
	configPath := c.String("config")
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}

	// File Locking
	unlock, err := utils.AcquireLock(cfg.LockFile)
	if err != nil {
		log.Fatalf("Could not acquire lock: %v", err)
	}
	defer unlock()

	log.Printf("Starting backup workflow using config: %s", configPath)

	// Initialize GORM
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port)

	sysDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	if err := sysDB.Exec("CREATE DATABASE IF NOT EXISTS sys_backup").Error; err != nil {
		log.Fatalf("Failed to create sys_backup database: %v", err)
	}

	dsnWithDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/sys_backup?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port)
	db, err := gorm.Open(mysql.Open(dsnWithDB), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to sys_backup database: %v", err)
	}

	if err := db.AutoMigrate(&model.BackupLog{}); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	notifier := notify.NewTelegramSender(cfg.Telegram.BotToken, cfg.Telegram.ChatID)

	r2Store, err := storage.NewStorage(cfg.R2)
	if err != nil {
		log.Fatalf("Failed to initialize MinIO/S3 storage: %v", err)
	}

	manager := backup.NewManager(cfg, r2Store, notifier, db)
	manager.OnlyDump = c.Bool("only-dump")

	if err := workflow(c.Context, manager); err != nil {
		log.Fatalf("Backup workflow failed: %v", err)
	}

	log.Println("Backup workflow completed successfully.")
	return nil
}
