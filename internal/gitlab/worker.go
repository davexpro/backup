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
	tempDir := filepath.Join(w.cfg.Backup.TempDir, fmt.Sprintf("gitlab_backup_%s", timestamp))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to create temp dir: %w", err)}
	}
	// Cleanup based on config
	if w.cfg.Backup.DeleteAfterUpload {
		defer os.RemoveAll(tempDir)
	} else {
		log.Printf("Keeping temp directory: %s", tempDir)
	}

	// 1. Trigger GitLab Backup via Rake
	log.Println("Triggering GitLab rake backup...")
	cmd := exec.CommandContext(ctx, "docker", "exec", w.cfg.GitLab.ContainerName, "gitlab-rake", "gitlab:backup:create")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("gitlab-rake failed: %w, output: %s", err, string(output))}
	}
	log.Printf("GitLab rake backup completed")

	// 2. Identify the backup file
	findCmd := exec.CommandContext(ctx, "docker", "exec", w.cfg.GitLab.ContainerName, "bash", "-c", "ls -t /var/opt/gitlab/backups/*_gitlab_backup.tar | head -1")
	findOutput, err := findCmd.CombinedOutput()
	if err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to find backup file in container: %w, output: %s", err, string(findOutput))}
	}
	remoteBackupPath := filepath.Clean(strings.TrimSpace(string(findOutput)))
	if remoteBackupPath == "" {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("no backup file found in container")}
	}
	backupFilename := filepath.Base(remoteBackupPath)
	log.Printf("Found backup file: %s", backupFilename)

	// 3. Copy files from container to host
	log.Printf("Copying backup file %s to host...", backupFilename)
	cpCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", w.cfg.GitLab.ContainerName, remoteBackupPath), tempDir)
	cpOutput, err := cpCmd.CombinedOutput()
	if err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("failed to copy backup file: %w, output: %s", err, string(cpOutput))}
	}

	log.Println("Copying GitLab configuration and secrets...")
	configFiles := []string{"/etc/gitlab/gitlab.rb", "/etc/gitlab/gitlab-secrets.json"}
	for _, f := range configFiles {
		cpFileCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", w.cfg.GitLab.ContainerName, f), tempDir)
		if cpErr := cpFileCmd.Run(); cpErr != nil {
			log.Printf("Warning: failed to copy %s: %v", f, cpErr)
		}
	}

	// 4. Zip & Encrypt all fetched files
	zipFilename := fmt.Sprintf("gitlab_backup_%s.zip", timestamp)
	localZipPath := filepath.Join(w.cfg.Backup.TempDir, zipFilename)

	if err := helper.ZipEncryptFolder(ctx, w.cfg.Encryption.Password, tempDir, localZipPath); err != nil {
		return helper.BackupResult{Database: "gitlab", Success: false, Error: fmt.Errorf("zip encryption failed: %w", err)}
	}
	// Cleanup zip based on config
	if w.cfg.Backup.DeleteAfterUpload {
		defer os.Remove(localZipPath)
	} else {
		log.Printf("Keeping zip file: %s", localZipPath)
	}

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
