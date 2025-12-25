package helper

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

// CalculateSHA256 calculates the SHA256 hash of a file.
func CalculateSHA256(path string) (string, int64, error) {
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
