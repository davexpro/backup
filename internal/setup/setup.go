package setup

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// checkAndInstallForDebian checks if mysqlsh is installed and installs it on Debian if missing.
func checkAndInstallForDebian() error {

	// 1. Check if mysqlsh is already installed
	if _, err := exec.LookPath("mysqlsh"); err == nil {
		log.Println("mysqlsh is already installed.")
		// We still ensure other utils are present
		_ = runAptInstall([]string{"zip", "unzip", "wget", "gnupg"})
		return nil
	}

	log.Println("mysqlsh not found. Starting installation on Debian-based system...")

	// 2. Install pre-requisites
	if err := runAptInstall([]string{"wget", "gnupg", "zip", "unzip", "lsb-release"}); err != nil {
		return fmt.Errorf("failed to install pre-requisites: %w", err)
	}

	// 3. Add MySQL APT Repository
	// Download the MySQL APT config package
	// https://dev.mysql.com/get/mysql-apt-config_0.8.36-1_all.deb
	repoPkg := "mysql-apt-config_0.8.36-1_all.deb"
	repoURL := "https://dev.mysql.com/get/" + repoPkg

	log.Printf("Downloading MySQL APT repository config from %s...", repoURL)
	wgetCmd := exec.Command("wget", "-O", "/tmp/"+repoPkg, repoURL)
	wgetCmd.Stdout = os.Stdout
	wgetCmd.Stderr = os.Stderr
	if err := wgetCmd.Run(); err != nil {
		return fmt.Errorf("failed to download mysql-apt-config: %w", err)
	}

	// Install the config package non-interactively
	log.Println("Installing MySQL APT repository config package...")
	// We use DEBIAN_FRONTEND=noninteractive to avoid prompts
	dpkgCmd := exec.Command("dpkg", "-i", "/tmp/"+repoPkg)
	dpkgCmd.Env = append(os.Environ(), "DEBIAN_FRONTEND=noninteractive")
	dpkgCmd.Stdout = os.Stdout
	dpkgCmd.Stderr = os.Stderr
	if err := dpkgCmd.Run(); err != nil {
		return fmt.Errorf("failed to install mysql-apt-config package: %w", err)
	}

	// 4. Update and Install mysql-shell
	if err := runAptInstall([]string{"mysql-shell"}); err != nil {
		return fmt.Errorf("failed to install mysql-shell: %w", err)
	}

	log.Println("mysqlsh installed successfully.")
	return nil
}

func runAptInstall(packages []string) error {
	log.Printf("Running apt-get update and installing: %s", strings.Join(packages, ", "))

	updateCmd := exec.Command("apt-get", "update")
	updateCmd.Stdout = os.Stdout
	updateCmd.Stderr = os.Stderr
	if err := updateCmd.Run(); err != nil {
		return fmt.Errorf("apt-get update failed: %w", err)
	}

	args := append([]string{"install", "-y"}, packages...)
	installCmd := exec.Command("apt-get", args...)
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	return installCmd.Run()
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
