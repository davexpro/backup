package helper

import (
	"context"
	"fmt"
	"os/exec"
)

// ZipEncrypt zips and encrypts a single file.
func ZipEncrypt(ctx context.Context, password, srcPath, dstPath string) error {
	args := []string{"-P", password, "-j", dstPath, srcPath}
	cmd := exec.CommandContext(ctx, "zip", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}

// ZipEncryptFolder zips and encrypts a folder.
func ZipEncryptFolder(ctx context.Context, password, srcDir, dstPath string) error {
	args := []string{"-P", password, "-r", "-j", dstPath, srcDir}
	cmd := exec.CommandContext(ctx, "zip", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("zip command failed: %w, output: %s", err, string(output))
	}
	return nil
}
