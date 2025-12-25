package backup

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/davexpro/backup/internal/utils"
)

func (m *Manager) BackupGitLab(ctx context.Context) error {
	log.Printf("Starting GitLab backup for container: %s", m.cfg.GitLab.ContainerName)
	start := time.Now()
	timestamp := start.Format("20060102_150405")
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("gitlab_backup_%s", timestamp))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Trigger GitLab Backup via Rake
	log.Println("Triggering GitLab rake backup...")
	cmd := exec.CommandContext(ctx, "docker", "exec", m.cfg.GitLab.ContainerName, "gitlab-rake", "gitlab:backup:create")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gitlab-rake failed: %w, output: %s", err, string(output))
	}

	// 2. Identify the backup file
	// GitLab backups are usually in /var/opt/gitlab/backups/ inside the container
	// We need to find the latest tar file created
	findCmd := exec.CommandContext(ctx, "docker", "exec", m.cfg.GitLab.ContainerName, "bash", "-c", "ls -t /var/opt/gitlab/backups/*_gitlab_backup.tar | head -1")
	output, err := findCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to find backup file in container: %w", err)
	}
	remoteBackupPath := string(output)
	remoteBackupPath = filepath.Clean(strings.TrimSpace(remoteBackupPath))
	if remoteBackupPath == "" {
		return fmt.Errorf("no backup file found in container")
	}
	backupFilename := filepath.Base(remoteBackupPath)

	// 3. Copy files from container to host
	log.Printf("Copying backup file %s to host...", backupFilename)
	cpCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", m.cfg.GitLab.ContainerName, remoteBackupPath), tempDir)
	if err := cpCmd.Run(); err != nil {
		return fmt.Errorf("failed to copy backup file: %w", err)
	}

	log.Println("Copying GitLab configuration and secrets...")
	configFiles := []string{"/etc/gitlab/gitlab.rb", "/etc/gitlab/gitlab-secrets.json"}
	for _, f := range configFiles {
		cpFileCmd := exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:%s", m.cfg.GitLab.ContainerName, f), tempDir)
		if err := cpFileCmd.Run(); err != nil {
			log.Printf("Warning: failed to copy %s: %v", f, err)
		}
	}

	// 4. Zip & Encrypt all fetched files
	zipFilename := fmt.Sprintf("gitlab_backup_%s.zip", timestamp)
	localZipPath := filepath.Join(os.TempDir(), zipFilename)

	// We zip the content of tempDir
	if err := m.zipEncryptFolder(ctx, tempDir, localZipPath); err != nil {
		return fmt.Errorf("zip encryption failed: %w", err)
	}
	defer os.Remove(localZipPath)

	// 5. Calculate SHA256
	hash, size, err := m.calculateSHA256(localZipPath)
	if err != nil {
		return fmt.Errorf("hash calc failed: %w", err)
	}

	// 6. Handle Upload or Local Save
	var uploadErr error
	if m.OnlyDump {
		localDir := "local_backups"
		os.MkdirAll(localDir, 0755)
		finalPath := filepath.Join(localDir, zipFilename)
		uploadErr = utils.CopyFile(localZipPath, finalPath)
		log.Printf("Saved GitLab backup locally to %s", finalPath)
	} else {
		file, err := os.Open(localZipPath)
		if err != nil {
			return fmt.Errorf("open file failed: %w", err)
		}
		defer file.Close()
		uploadErr = m.storage.Upload(ctx, zipFilename, file)
	}

	// 7. Log Result
	res := BackupResult{
		Database: "gitlab",
		Success:  uploadErr == nil,
		Size:     size,
		SHA256:   hash,
		Error:    uploadErr,
		Duration: time.Since(start),
	}
	m.logResult(res)
	m.sendReport([]BackupResult{res}, 1, 0) // Simplified report for GitLab

	return uploadErr
}

func (m *Manager) zipEncryptFolder(ctx context.Context, srcDir, dstPath string) error {
	// zip -P <password> -r -j <dst> <srcDir>
	args := []string{"-P", m.cfg.Encryption.Password, "-r", "-j", dstPath, srcDir}
	cmd := exec.CommandContext(ctx, "zip", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}
