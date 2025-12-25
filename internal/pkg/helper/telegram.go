package helper

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
)

type TelegramSender struct {
	BotToken string
	ChatID   string
	Client   *http.Client
}

func NewTelegramSender(botToken, chatID string) *TelegramSender {
	return &TelegramSender{
		BotToken: botToken,
		ChatID:   chatID,
		Client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *TelegramSender) Send(message string) error {
	if s.BotToken == "" || s.ChatID == "" {
		return nil // Notification disabled
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", s.BotToken)

	payload := map[string]string{
		"chat_id": s.ChatID,
		"text":    message,
	}

	jsonData, err := sonic.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal notification payload: %w", err)
	}

	resp, err := s.Client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send telegram message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram api returned non-200 status: %d", resp.StatusCode)
	}

	return nil
}
