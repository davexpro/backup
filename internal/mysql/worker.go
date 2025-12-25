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

// Worker handles MySQL backup and recovery operations.
type Worker struct {
	cfg      *config.Config
	store    *helper.Storage
	notifier *helper.TelegramSender
	onlyDump bool
}

// NewWorker creates a new MySQL worker.
func NewWorker(cfg *config.Config, store *helper.Storage, notifier *helper.TelegramSender, onlyDump bool) *Worker {
	return &Worker{
		cfg:      cfg,
		store:    store,
		notifier: notifier,
		onlyDump: onlyDump,
	}
}

// Backup executes the MySQL backup workflow.
func (w *Worker) Backup(ctx context.Context) error {
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

	timeNow := time.Now()
	for _, dbName := range databases {
		if w.shouldExcludeDB(dbName) {
			log.Printf("Skipping excluded database: %s", dbName)
			continue
		}

		log.Printf("Backing up database: %s", dbName)
		start := time.Now()
		result := w.backupDatabase(ctx, dbName, timeNow)
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

// Recover restores data from a dump path (directory or zip).
func (w *Worker) Recover(ctx context.Context, inputPath string) error {
	log.Printf("Starting recovery from: %s", inputPath)

	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("failed to access input path: %w", err)
	}

	dumpDir := inputPath
	isZip := !info.IsDir() && strings.HasSuffix(strings.ToLower(inputPath), ".zip")

	if isZip {
		log.Printf("Detecting zip file, extracting to temporary directory...")
		tempRestoreDir := filepath.Join(w.cfg.Backup.TempDir, fmt.Sprintf("restore_%d", time.Now().Unix()))
		if err := os.MkdirAll(tempRestoreDir, 0755); err != nil {
			return fmt.Errorf("failed to create temp restore dir: %w", err)
		}
		defer os.RemoveAll(tempRestoreDir)

		// Unzip logic (using system unzip or our helper if we add it)
		// For now using shell unzip as it's common and supports pwd
		unzipArgs := []string{"-o", inputPath, "-d", tempRestoreDir}
		if w.cfg.Encryption.Password != "" {
			unzipArgs = append([]string{"-P", w.cfg.Encryption.Password}, unzipArgs...)
		}

		log.Printf("Executing unzip %v", unzipArgs)
		unzipCmd := exec.CommandContext(ctx, "unzip", unzipArgs...)
		if output, err := unzipCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("unzip failed: %w, output: %s", err, string(output))
		}

		// The zip might contain a subfolder (like dbname_timestamp) or direct files
		// mysqlsh util.loadDump needs the directory containing the @.json metadata
		dumpDir = tempRestoreDir

		// Look for subfolders if the root of unzip doesn't have @.json
		if _, err := os.Stat(filepath.Join(dumpDir, "@.json")); os.IsNotExist(err) {
			entries, _ := os.ReadDir(dumpDir)
			for _, entry := range entries {
				if entry.IsDir() {
					subDir := filepath.Join(dumpDir, entry.Name())
					if _, err := os.Stat(filepath.Join(subDir, "@.json")); err == nil {
						dumpDir = subDir
						break
					}
				}
			}
		}
	}

	if _, err := os.Stat(filepath.Join(dumpDir, "@.json")); os.IsNotExist(err) {
		return fmt.Errorf("dump metadata (@.json) not found in %s", dumpDir)
	}

	log.Printf("Restoring from directory: %s", dumpDir)

	// util.loadDump(path, {threads: N, ignoreVersion: true, ...})
	loadOpts := fmt.Sprintf("{threads: %d, ignoreVersion: true}", w.cfg.MySQL.Threads)
	script := fmt.Sprintf("util.loadDump('%s', %s)", dumpDir, loadOpts)

	args := []string{
		fmt.Sprintf("--user=%s", w.cfg.MySQL.User),
		fmt.Sprintf("--password=%s", w.cfg.MySQL.Password),
		fmt.Sprintf("--host=%s", w.cfg.MySQL.Host),
		fmt.Sprintf("--port=%d", w.cfg.MySQL.Port),
		"--js",
		"-e",
		script,
	}

	log.Printf("Executing mysqlsh recovery script...")
	cmd := exec.CommandContext(ctx, "mysqlsh", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mysqlsh recovery failed: %w, output: %s", err, string(output))
	}

	log.Printf("Recovery completed successfully:\n%s", string(output))
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
	var filtered []string
	for _, dbName := range databases {
		if strings.Contains(dbName, "WARNING:") {
			continue
		}
		filtered = append(filtered, dbName)
	}
	if len(w.cfg.MySQL.Include) > 0 {
		var includedOnly []string
		for _, db := range filtered {
			for _, inc := range w.cfg.MySQL.Include {
				if db == inc {
					includedOnly = append(includedOnly, db)
					break
				}
			}
		}
		return includedOnly
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

func (w *Worker) backupDatabase(ctx context.Context, dbName string, timeNow time.Time) helper.BackupResult {
	timestamp := timeNow.Format("20060102_150405")
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
