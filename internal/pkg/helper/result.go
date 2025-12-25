package helper

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// BackupResult holds the result of a single backup operation.
type BackupResult struct {
	Database string
	Success  bool
	Size     int64
	SHA256   string
	Error    error
	Duration time.Duration
}

// SendReport sends a backup report via Telegram.
func SendReport(notifier *TelegramSender, results []BackupResult, success, fail int) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Backup Report [%s]\n", time.Now().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Total: %d, Success: %d, Fail: %d\n\n", len(results), success, fail))

	for _, res := range results {
		if res.Success {
			sb.WriteString(fmt.Sprintf("✅ %s: %s (SHA256: %s...)\n", res.Database, HumanizeSize(res.Size), res.SHA256[:8]))
		} else {
			sb.WriteString(fmt.Sprintf("❌ %s: Error: %v\n", res.Database, res.Error))
		}
	}

	if err := notifier.Send(sb.String()); err != nil {
		log.Printf("Failed to send telegram notification: %v", err)
	}
}
