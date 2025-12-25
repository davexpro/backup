package helper

import (
	"fmt"
	"os/exec"
	"strings"
)

// CheckTools verifies that the required command-line tools are available in the system PATH.
func CheckTools(tools ...string) error {
	var missing []string
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("required tools missing: %s. Please run 'backup setup' (on Debian/Ubuntu) or install them manually", strings.Join(missing, ", "))
	}
	return nil
}
