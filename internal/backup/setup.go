package backup

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// CheckAndInstallMySQLShell checks if mysqlsh is installed and installs it on Debian if missing.
func CheckAndInstallMySQLShell() error {
	// 1. OS Check (Debian only)
	if !isDebian() {
		return fmt.Errorf("the setup command is only supported on Debian-based systems")
	}

	// 2. Check if mysqlsh is already installed
	if _, err := exec.LookPath("mysqlsh"); err == nil {
		log.Println("mysqlsh is already installed.")
		return nil
	}

	log.Println("mysqlsh not found. Starting installation on Debian...")

	// 3. Install mysqlsh
	// Commands for Debian/Ubuntu:
	// We need to add the MySQL repo or use the default one if available.
	// For simplicity, we try apt-get install mysql-shell directly.
	// In many cases, it might need the official MySQL APT repository.

	installCmds := [][]string{
		{"apt-get", "update"},
		{"apt-get", "install", "-y", "mysql-shell"},
	}

	for _, args := range installCmds {
		log.Printf("Running: %s %s", args[0], strings.Join(args[1:], " "))
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to run %s: %w", args[0], err)
		}
	}

	log.Println("mysqlsh installed successfully.")
	return nil
}

func isDebian() bool {
	// Check /etc/os-release
	data, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return false
	}
	content := string(data)
	// Look for ID=debian or ID_LIKE=debian
	return strings.Contains(content, "ID=debian") ||
		strings.Contains(content, "ID_LIKE=debian") ||
		strings.Contains(content, "ID=ubuntu") ||
		strings.Contains(content, "ID_LIKE=ubuntu")
}
