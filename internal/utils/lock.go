package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

// AcquireLock attempts to acquire a file lock.
// It returns a release function and an error if it fails to acquire the lock immediately.
func AcquireLock(lockPath string) (func(), error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(lockPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create lock directory: %w", err)
	}

	fileLock := flock.New(lockPath)

	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to attempt lock: %w", err)
	}

	if !locked {
		return nil, fmt.Errorf("lock file %s is already locked, another instance might be running", lockPath)
	}

	return func() {
		fileLock.Unlock()
	}, nil
}
