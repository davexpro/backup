package mysql

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/davexpro/backup/internal/config"
	"github.com/davexpro/backup/internal/pkg/helper"
)

// Worker handles MySQL backup operations.
type Worker struct {
	cfg      *config.Config
	store    *helper.Storage
	notifier *helper.TelegramSender
	onlyDump bool
}

// NewWorker creates a new MySQL backup worker.
func NewWorker(cfg *config.Config, store *helper.Storage, notifier *helper.TelegramSender, onlyDump bool) *Worker {
	return &Worker{
		cfg:      cfg,
		store:    store,
		notifier: notifier,
		onlyDump: onlyDump,
	}
}

// Run executes the MySQL backup workflow.
func (w *Worker) Run(ctx context.Context) error {
	// List databases using mysqlsh
	databases, err := w.listDatabases(ctx)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}

	var results []helper.BackupResult
	var successCount, failCount int

	for _, dbName := range databases {
		if w.shouldExclude(dbName) {
			log.Printf("Skipping excluded database: %s", dbName)
			continue
		}

		log.Printf("Backing up database: %s", dbName)
		start := time.Now()
		result := w.backupDatabase(ctx, dbName)
		result.Duration = time.Since(start)

		if result.Success {
			successCount++
			log.Printf("Backup success: %s (Size: %d bytes, SHA256: %s)", dbName, result.Size, result.SHA256)
		} else {
			failCount++
			log.Printf("Backup failed: %s (%v)", dbName, result.Error)
		}
		results = append(results, result)
	}

	// Enforce retention
	if err := w.store.EnforceRetention(ctx, w.cfg.Retention.Hours); err != nil {
		log.Printf("Error enforcing retention policy: %v", err)
	}

	helper.SendReport(w.notifier, results, successCount, failCount)

	if failCount > 0 {
		return fmt.Errorf("backup completed with %d failures", failCount)
	}
	return nil
}

func (w *Worker) listDatabases(ctx context.Context) ([]string, error) {
	args := []string{
		fmt.Sprintf("--user=%s", w.cfg.MySQL.User),
		fmt.Sprintf("--password=%s", w.cfg.MySQL.Password),
		fmt.Sprintf("--host=%s", w.cfg.MySQL.Host),
		fmt.Sprintf("--port=%d", w.cfg.MySQL.Port),
		"--batch",
		"-e",
		"SELECT schema_name FROM information_schema.schemata",
	}

	cmd := exec.CommandContext(ctx, "mysqlsh", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("mysqlsh list databases failed: %w", err)
	}

	var databases []string
	lines := filepath.SplitList(string(output))
	// Parse output - mysqlsh returns schema names one per line
	for _, line := range splitLines(string(output)) {
		line = trimSpace(line)
		if line != "" && line != "schema_name" {
			databases = append(databases, line)
		}
	}
	_ = lines // unused
	return databases, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}

func (w *Worker) shouldExclude(dbName string) bool {
	systemDBs := []string{"information_schema", "performance_schema", "mysql", "sys"}
	for _, sys := range systemDBs {
		if dbName == sys {
			return true
		}
	}
	for _, excl := range w.cfg.MySQL.Exclude {
		if dbName == excl {
			return true
		}
	}
	return false
}

func (w *Worker) backupDatabase(ctx context.Context, dbName string) helper.BackupResult {
	timestamp := time.Now().Format("20060102_150405")
	dumpDir := filepath.Join(os.TempDir(), fmt.Sprintf("%s_%s", dbName, timestamp))

	zipFilename := fmt.Sprintf("%s_%s.zip", dbName, timestamp)
	localZipPath := filepath.Join(os.TempDir(), zipFilename)

	if err := w.dump(ctx, dbName, dumpDir); err != nil {
		return helper.BackupResult{Database: dbName, Success: false, Error: err}
	}
	defer os.RemoveAll(dumpDir)

	if err := helper.ZipEncryptFolder(ctx, w.cfg.Encryption.Password, dumpDir, localZipPath); err != nil {
		return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("zip encryption failed: %w", err)}
	}
	defer os.Remove(localZipPath)

	hash, size, err := helper.CalculateSHA256(localZipPath)
	if err != nil {
		return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("hash calc failed: %w", err)}
	}

	if w.onlyDump {
		localDir := "local_backups"
		if err := os.MkdirAll(localDir, 0755); err != nil {
			return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("failed to create local backup dir: %w", err)}
		}
		finalPath := filepath.Join(localDir, zipFilename)
		if err := helper.CopyFile(localZipPath, finalPath); err != nil {
			return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("failed to save local backup: %w", err)}
		}
		log.Printf("Saved backup locally to %s", finalPath)
	} else {
		file, err := os.Open(localZipPath)
		if err != nil {
			return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("open file failed: %w", err)}
		}
		defer file.Close()

		if err := w.store.Upload(ctx, zipFilename, file); err != nil {
			return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("upload failed: %w", err)}
		}
	}

	return helper.BackupResult{
		Database: dbName,
		Success:  true,
		Size:     size,
		SHA256:   hash,
	}
}

func (w *Worker) dump(ctx context.Context, dbName, outputPath string) error {
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return err
	}
	args := []string{
		fmt.Sprintf("--user=%s", w.cfg.MySQL.User),
		fmt.Sprintf("--password=%s", w.cfg.MySQL.Password),
		fmt.Sprintf("--host=%s", w.cfg.MySQL.Host),
		fmt.Sprintf("--port=%d", w.cfg.MySQL.Port),
		"--batch",
		"-e",
		fmt.Sprintf("util.dumpSchemas(['%s'], '%s', {threads: 4, compression: 'none'})", dbName, outputPath),
	}

	cmd := exec.CommandContext(ctx, "mysqlsh", args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysqlsh dump failed: %w", err)
	}
	return nil
}
