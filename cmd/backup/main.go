package main

import (
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
		},
		Action: func(c *cli.Context) error {
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
			// Connect to server (no DB selected initially to list DBs, but for log table we need a DB)
			// Strategy: Connect without DB to list, but we need to store logs somewhere.
			// Let's assume we store logs in a specific DB called 'sys_backup' or we use 'mysql' provided we have permissions,
			// or we just pick one. Ideally we should create a 'sys_backup' DB if not exists.

			// Connect without DB to create sys_backup if needed
			sysDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
			if err != nil {
				log.Fatalf("Failed to connect to MySQL: %v", err)
			}

			// Create sys_backup database if not exists
			if err := sysDB.Exec("CREATE DATABASE IF NOT EXISTS sys_backup").Error; err != nil {
				log.Fatalf("Failed to create sys_backup database: %v", err)
			}

			// Reconnect to sys_backup
			dsnWithDB := fmt.Sprintf("%s:%s@tcp(%s:%d)/sys_backup?charset=utf8mb4&parseTime=True&loc=Local",
				cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port)
			db, err := gorm.Open(mysql.Open(dsnWithDB), &gorm.Config{})
			if err != nil {
				log.Fatalf("Failed to connect to sys_backup database: %v", err)
			}

			// AutoMigrate
			if err := db.AutoMigrate(&model.BackupLog{}); err != nil {
				log.Fatalf("Failed to migrate database: %v", err)
			}

			// Initialize dependencies
			notifier := notify.NewTelegramSender(cfg.Telegram.BotToken, cfg.Telegram.ChatID)

			r2Store, err := storage.NewStorage(cfg.R2)
			if err != nil {
				log.Fatalf("Failed to initialize MinIO/S3 storage: %v", err)
			}

			manager := backup.NewManager(cfg, r2Store, notifier, db)

			// Run Backup Workflow
			if err := manager.Run(c.Context); err != nil {
				// manager.Run sends notifications on its own, but we log here too
				log.Fatalf("Backup workflow failed: %v", err)
			}

			log.Println("Backup workflow completed successfully.")
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
