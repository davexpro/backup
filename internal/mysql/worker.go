package mysql

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	// Filter databases based on include list
	databases = w.filterDatabases(databases)
	log.Printf("Databases to backup: %v", databases)

	var results []helper.BackupResult
	var successCount, failCount int

	for _, dbName := range databases {
		if w.shouldExcludeDB(dbName) {
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
		"--sql",
		"-e",
		"SELECT schema_name FROM information_schema.schemata",
	}

	log.Printf("Listing databases...")
	cmd := exec.CommandContext(ctx, "mysqlsh", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("mysqlsh list databases failed: %w, output: %s", err, string(output))
	}

	var databases []string
	// Parse output - filter out warnings, headers, and empty lines
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		// Skip empty lines, warnings, headers, and separator lines
		if line == "" ||
			strings.HasPrefix(line, "WARNING:") ||
			strings.HasPrefix(line, "SCHEMA_NAME") ||
			strings.HasPrefix(line, "schema_name") ||
			strings.HasPrefix(line, "+") ||
			strings.HasPrefix(line, "|") {
			continue
		}
		databases = append(databases, line)
	}

	log.Printf("Found databases: %v", databases)
	return databases, nil
}

// filterDatabases filters databases based on include list
func (w *Worker) filterDatabases(databases []string) []string {
	// If include list is specified, only include those databases
	var filtered []string
	for _, dbName := range databases {
		if strings.Contains(dbName, "WARNING:") {
			continue
		}
		filtered = append(filtered, dbName)
	}
	if len(w.cfg.MySQL.Include) > 0 {
		for _, db := range filtered {
			for _, inc := range w.cfg.MySQL.Include {
				if db == inc {
					filtered = append(filtered, db)
					break
				}
			}
		}
	}
	return filtered
}

// shouldExcludeDB checks if a database should be excluded
func (w *Worker) shouldExcludeDB(dbName string) bool {
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
	dumpDir := filepath.Join(w.cfg.Backup.TempDir, fmt.Sprintf("%s_%s", dbName, timestamp))

	zipFilename := fmt.Sprintf("%s_%s.zip", dbName, timestamp)
	localZipPath := filepath.Join(w.cfg.Backup.TempDir, zipFilename)

	if err := w.dump(ctx, dbName, dumpDir); err != nil {
		return helper.BackupResult{Database: dbName, Success: false, Error: err}
	}
	// Cleanup dump directory based on config
	if w.cfg.Backup.DeleteAfterUpload {
		defer os.RemoveAll(dumpDir)
	} else {
		log.Printf("Keeping dump directory: %s", dumpDir)
	}

	if err := helper.ZipEncryptFolder(ctx, w.cfg.Encryption.Password, dumpDir, localZipPath); err != nil {
		return helper.BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("zip encryption failed: %w", err)}
	}
	// Cleanup zip file based on config
	if w.cfg.Backup.DeleteAfterUpload {
		defer os.Remove(localZipPath)
	} else {
		log.Printf("Keeping zip file: %s", localZipPath)
	}

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

	// Build dump options
	dumpOpts := w.buildDumpOptions(dbName, outputPath)

	// Use --js for JavaScript mode since util.dumpSchemas is a JS function
	args := []string{
		fmt.Sprintf("--user=%s", w.cfg.MySQL.User),
		fmt.Sprintf("--password=%s", w.cfg.MySQL.Password),
		fmt.Sprintf("--host=%s", w.cfg.MySQL.Host),
		fmt.Sprintf("--port=%d", w.cfg.MySQL.Port),
		"--js",
		"-e",
		dumpOpts,
	}

	log.Printf("Dumping database %s to %s", dbName, outputPath)
	cmd := exec.CommandContext(ctx, "mysqlsh", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mysqlsh dump failed: %w, output: %s", err, string(output))
	}
	log.Printf("Dump completed for %s", dbName)
	return nil
}

// buildDumpOptions builds the util.dumpSchemas command with table filtering support using JS logic
func (w *Worker) buildDumpOptions(dbName, outputPath string) string {
	threads := w.cfg.MySQL.Threads
	filters := w.cfg.MySQL.TableFilters

	// Escape strings for JS
	jsIncludeTables := "[]"
	if len(filters.Include) > 0 {
		var entries []string
		for _, t := range filters.Include {
			entries = append(entries, fmt.Sprintf("'%s.%s'", dbName, t))
		}
		jsIncludeTables = "[" + strings.Join(entries, ", ") + "]"
	}

	jsExcludeTables := "[]"
	if len(filters.Exclude) > 0 {
		var entries []string
		for _, t := range filters.Exclude {
			entries = append(entries, fmt.Sprintf("'%s.%s'", dbName, t))
		}
		jsExcludeTables = "[" + strings.Join(entries, ", ") + "]"
	}

	jsIncludePrefixes := "[]"
	if len(filters.IncludePrefix) > 0 {
		var entries []string
		for _, p := range filters.IncludePrefix {
			entries = append(entries, fmt.Sprintf("'%s'", p))
		}
		jsIncludePrefixes = "[" + strings.Join(entries, ", ") + "]"
	}

	jsExcludePrefixes := "[]"
	if len(filters.ExcludePrefix) > 0 {
		var entries []string
		for _, p := range filters.ExcludePrefix {
			entries = append(entries, fmt.Sprintf("'%s'", p))
		}
		jsExcludePrefixes = "[" + strings.Join(entries, ", ") + "]"
	}

	// Dynamic script to calculate table lists based on prefixes
	script := fmt.Sprintf(`
var db = '%s';
var includeTables = %s;
var excludeTables = %s;
var includePrefixes = %s;
var excludePrefixes = %s;

includePrefixes.forEach(function(p) {
    var rs = session.runSql("SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_name LIKE ?", [db, p + "%%"]);
    rs.fetchAll().forEach(function(row) { includeTables.push(db + "." + row[0]); });
});

excludePrefixes.forEach(function(p) {
    var rs = session.runSql("SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_name LIKE ?", [db, p + "%%"]);
    rs.fetchAll().forEach(function(row) { excludeTables.push(db + "." + row[0]); });
});

var opts = {threads: %d, compression: 'zstd'};
if (includeTables.length > 0) opts.includeTables = includeTables;
if (excludeTables.length > 0) opts.excludeTables = excludeTables;

util.dumpSchemas([db], '%s', opts);
`, dbName, jsIncludeTables, jsExcludeTables, jsIncludePrefixes, jsExcludePrefixes, threads, outputPath)

	// Clean up script for logging and execution (remove newlines for -e if necessary, but mysqlsh supports multidatabase scripts)
	log.Printf("Generated mysqlsh JS script for %s", dbName)
	return script
}
