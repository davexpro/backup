package backup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/davexpro/backup/internal/config"
	model "github.com/davexpro/backup/internal/db"
	"github.com/davexpro/backup/internal/notify"
	"github.com/davexpro/backup/internal/storage"
	"github.com/davexpro/backup/internal/utils"
	"gorm.io/gorm"
)

type Manager struct {
	cfg      *config.Config
	storage  *storage.Storage
	notifier *notify.TelegramSender
	db       *gorm.DB
	OnlyDump bool
}

func NewManager(cfg *config.Config, store *storage.Storage, notifier *notify.TelegramSender, db *gorm.DB) *Manager {
	return &Manager{
		cfg:      cfg,
		storage:  store,
		notifier: notifier,
		db:       db,
		OnlyDump: false,
	}
}

type BackupResult struct {
	Database string
	Success  bool
	Size     int64
	SHA256   string
	Error    error
	Duration time.Duration
}

func (m *Manager) Run(ctx context.Context) error {
	dbs, err := m.listDatabases()
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}

	var results []BackupResult
	var successCount, failCount int

	for _, db := range dbs {
		if m.shouldExclude(db) {
			log.Printf("Skipping excluded database: %s", db)
			continue
		}

		log.Printf("Backing up database: %s", db)
		start := time.Now()
		result := m.backupDatabase(ctx, db)
		result.Duration = time.Since(start)

		if result.Success {
			successCount++
			log.Printf("Backup success: %s (Size: %d bytes, SHA256: %s)", db, result.Size, result.SHA256)
		} else {
			failCount++
			log.Printf("Backup failed: %s (%v)", db, result.Error)
		}
		results = append(results, result)

		// Log to Database using GORM
		m.logResult(result)
	}

	// Enforce retention
	if err := m.storage.EnforceRetention(ctx, m.cfg.Retention.Hours); err != nil {
		log.Printf("Error enforcing retention policy: %v", err)
	}

	// Send notification
	m.sendReport(results, successCount, failCount)

	if failCount > 0 {
		return fmt.Errorf("backup completed with %d failures", failCount)
	}
	return nil
}

func (m *Manager) listDatabases() ([]string, error) {
	var databases []string
	if err := m.db.Raw("SHOW DATABASES").Scan(&databases).Error; err != nil {
		return nil, err
	}
	return databases, nil
}

func (m *Manager) logResult(res BackupResult) {
	status := "SUCCESS"
	errorMsg := ""
	if !res.Success {
		status = "FAILED"
		errorMsg = res.Error.Error()
	}

	logEntry := model.BackupLog{
		Database: res.Database,
		Status:   status,
		Size:     res.Size,
		SHA256:   res.SHA256,
		Error:    errorMsg,
		Duration: res.Duration.Seconds(),
	}

	if err := m.db.Create(&logEntry).Error; err != nil {
		log.Printf("Failed to log backup history for %s: %v", res.Database, err)
	}
}

func (m *Manager) shouldExclude(db string) bool {
	// Exclude our own log database if we use a dedicated one,
	// but usually we use 'sys_backup' or something.
	// If the user connects to MySQL root, they can access all DBs.
	// We should probably exclude the table's DB if it's in the list.
	// Assuming the GORM connection is to a specific DB or server root.
	// Typically SHOW DATABASES lists everything.

	systemDBs := []string{"information_schema", "performance_schema", "mysql", "sys"}
	for _, sys := range systemDBs {
		if db == sys {
			return true
		}
	}
	for _, excl := range m.cfg.MySQL.Exclude {
		if db == excl {
			return true
		}
	}
	// TODO: Maybe exclude the DB where backup_logs are stored if necessary?
	// For now relying on user config.
	return false
}

func (m *Manager) backupDatabase(ctx context.Context, dbName string) BackupResult {
	timestamp := time.Now().Format("20060102_150405")
	sqlFilename := fmt.Sprintf("%s_%s.sql", dbName, timestamp)
	localSQLPath := filepath.Join(os.TempDir(), sqlFilename)

	zipFilename := fmt.Sprintf("%s_%s.zip", dbName, timestamp)
	localZipPath := filepath.Join(os.TempDir(), zipFilename)

	// 1. Dump to SQL using mysqlsh
	if err := m.dump(ctx, dbName, localSQLPath); err != nil {
		return BackupResult{Database: dbName, Success: false, Error: err}
	}
	defer os.RemoveAll(localSQLPath) // mysqlsh dumps to a directory

	// 2. Zip & Encrypt
	if err := m.zipEncrypt(ctx, localSQLPath, localZipPath); err != nil {
		return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("zip encryption failed: %w", err)}
	}
	defer os.Remove(localZipPath) // Clean up temp Zip file

	// 3. Calculate SHA256
	hash, size, err := m.calculateSHA256(localZipPath)
	if err != nil {
		return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("hash calc failed: %w", err)}
	}

	// 4. Handle Upload or Local Save
	if m.OnlyDump {
		localDir := "local_backups"
		if err := os.MkdirAll(localDir, 0755); err != nil {
			return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("failed to create local backup dir: %w", err)}
		}
		finalPath := filepath.Join(localDir, zipFilename)
		if err := utils.CopyFile(localZipPath, finalPath); err != nil {
			return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("failed to save local backup: %w", err)}
		}
		log.Printf("Saved backup locally to %s", finalPath)
	} else {
		file, err := os.Open(localZipPath)
		if err != nil {
			return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("open file failed: %w", err)}
		}
		defer file.Close()

		if err := m.storage.Upload(ctx, zipFilename, file); err != nil {
			return BackupResult{Database: dbName, Success: false, Error: fmt.Errorf("upload failed: %w", err)}
		}
	}

	return BackupResult{
		Database: dbName,
		Success:  true,
		Size:     size,
		SHA256:   hash,
	}
}

func (m *Manager) dump(ctx context.Context, dbName, outputPath string) error {
	// mysqlsh uses a directory for dumping
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return err
	}

	// mysqlsh -u <user> -p<pass> -h <host> -P <port> -- util dump-schemas <schema> <path> --threads=4
	// Using --util dump-schemas instead of dump-instance for specific database
	args := []string{
		fmt.Sprintf("--user=%s", m.cfg.MySQL.User),
		fmt.Sprintf("--password=%s", m.cfg.MySQL.Password),
		fmt.Sprintf("--host=%s", m.cfg.MySQL.Host),
		fmt.Sprintf("--port=%d", m.cfg.MySQL.Port),
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

func (m *Manager) zipEncrypt(ctx context.Context, srcPath, dstPath string) error {
	// zip -P <password> -j <dst> <src>
	// -j: store just the name of a saved file (junk the path)
	args := []string{"-P", m.cfg.Encryption.Password, "-j", dstPath, srcPath}

	cmd := exec.CommandContext(ctx, "zip", args...)
	// Capture output for error reasoning, but be careful with logs if we printed args (we don't here)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (m *Manager) calculateSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()

	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), size, nil
}

func (m *Manager) sendReport(results []BackupResult, success, fail int) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Backup Report [%s]\n", time.Now().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Total: %d, Success: %d, Fail: %d\n\n", len(results), success, fail))

	for _, res := range results {
		if res.Success {
			sb.WriteString(fmt.Sprintf("✅ %s: %s (SHA256: %s...)\n", res.Database, humanizeSize(res.Size), res.SHA256[:8]))
		} else {
			sb.WriteString(fmt.Sprintf("❌ %s: Error: %v\n", res.Database, res.Error))
		}
	}

	if err := m.notifier.Send(sb.String()); err != nil {
		log.Printf("Failed to send telegram notification: %v", err)
	}
}

func humanizeSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
