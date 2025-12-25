package gitlab

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

// Worker handles GitLab backup operations.
type Worker struct {
	cfg      *config.Config
	store    *helper.Storage
	notifier *helper.TelegramSender
	onlyDump bool
}

// NewWorker creates a new GitLab backup worker.
func NewWorker(cfg *config.Config, store *helper.Storage, notifier *helper.TelegramSender, onlyDump bool) *Worker {
	return &Worker{
		cfg:      cfg,
		store:    store,
		notifier: notifier,
		onlyDump: onlyDump,
	}
}

// Run executes the GitLab backup workflow.
func (w *Worker) Run(ctx context.Context) error {
	start := time.Now()
	result := w.backup(ctx)
	result.Duration = time.Since(start)

	helper.SendReport(w.notifier, []helper.BackupResult{result}, 1, 0)

	if !result.Success {
		return fmt.Errorf("GitLab backup failed: %v", result.Error)
	}
	return nil
}

func (w *Worker) backup(ctx context.Context) helper.BackupResult {
	start := time.Now()
	timestamp := start.Format("20060102_150405")
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("gitlab_backup_%s", timestamp))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to create temp dir: %w", err)}
	}
	defer os.RemoveAll(tempDir)

	// 1. Trigger GitLab Backup via Rake
	log.Println("Triggering GitLab rake backup...")
	cmd := exec.CommandContext(ctx, "docker", "exec", w.cfg.GitLab.ContainerName, "gitlab-rake", "gitlab:backup:create")
	if output, err := cmd.CombinedOutput(); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("gitlab-rake failed: %w, output: %s", err, string(output))}
	}

	// 2. Identify the backup file
	findCmd := exec.CommandContext(ctx, "docker", "exec", w.cfg.GitLab.ContainerName, "bash", "-c", "ls -t /var/opt/gitlab/backups/*_gitlab_backup.tar | head -1")
	output, err := findCmd.Output()
	if err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to find backup file in container: %w", err)}
	}
	remoteBackupPath := filepath.Clean(strings.TrimSpace(string(output)))
	if remoteBackupPath == "" {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("no backup file found in container")}
	}
	backupFilename := filepath.Base(remoteBackupPath)

	// 3. Copy files from container to host
	log.Printf("Copying backup file %s to host...", backupFilename)
	cpCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", w.cfg.GitLab.ContainerName, remoteBackupPath), tempDir)
	if err := cpCmd.Run(); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to copy backup file: %w", err)}
	}

	log.Println("Copying GitLab configuration and secrets...")
	configFiles := []string{"/etc/gitlab/gitlab.rb", "/etc/gitlab/gitlab-secrets.json"}
	for _, f := range configFiles {
		cpFileCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", w.cfg.GitLab.ContainerName, f), tempDir)
		if err := cpFileCmd.Run(); err != nil {
			log.Printf("Warning: failed to copy %s: %v", f, err)
		}
	}

	// 4. Zip & Encrypt all fetched files
	zipFilename := fmt.Sprintf("gitlab_backup_%s.zip", timestamp)
	localZipPath := filepath.Join(os.TempDir(), zipFilename)

	if err := helper.ZipEncryptFolder(ctx, w.cfg.Encryption.Password, tempDir, localZipPath); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("zip encryption failed: %w", err)}
	}
	defer os.Remove(localZipPath)

	// 5. Calculate SHA256
	hash, size, err := helper.CalculateSHA256(localZipPath)
	if err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("hash calc failed: %w", err)}
	}

	// 6. Handle Upload or Local Save
	var uploadErr error
	if w.onlyDump {
		localDir := "local_backups"
		os.MkdirAll(localDir, 0755)
		finalPath := filepath.Join(localDir, zipFilename)
		uploadErr = helper.CopyFile(localZipPath, finalPath)
		log.Printf("Saved GitLab backup locally to %s", finalPath)
	} else {
		file, err := os.Open(localZipPath)
		if err != nil {
			return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("open file failed: %w", err)}
		}
		defer file.Close()
		uploadErr = w.store.Upload(ctx, zipFilename, file)
	}

	return helper.BackupResult{
		Database: "gitlab",
		Success:  uploadErr == nil,
		Size:     size,
		SHA256:   hash,
		Error:    uploadErr,
	}
}
