package helper

import (
	"context"
	"fmt"
	"os/exec"
)

// ZipEncrypt zips and potentially encrypts a single file.
func ZipEncrypt(ctx context.Context, password, srcPath, dstPath string) error {
	args := []string{"-j"}
	if password != "" {
		args = append([]string{"-P", password}, args...)
	}
	args = append(args, dstPath, srcPath)

	cmd := exec.CommandContext(ctx, "zip", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}

// ZipEncryptFolder zips and potentially encrypts a folder.
func ZipEncryptFolder(ctx context.Context, password, srcDir, dstPath string) error {
	args := []string{"-r", "-j"}
	if password != "" {
		args = append([]string{"-P", password}, args...)
	}
	args = append(args, dstPath, srcDir)

	cmd := exec.CommandContext(ctx, "zip", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}
