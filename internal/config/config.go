package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration.
type Config struct {
	MySQL      MySQLConfig      `yaml:"mysql"`
	R2         R2Config         `yaml:"r2"`
	Retention  RetentionConfig  `yaml:"retention"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Telegram   TelegramConfig   `yaml:"telegram"`
	LockFile   string           `yaml:"lock_file"`
}

type MySQLConfig struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Exclude  []string `yaml:"exclude"` // List of databases to exclude (optional)
}

type R2Config struct {
	Endpoint   string `yaml:"endpoint"`
	AccessKey  string `yaml:"access_key"`
	SecretKey  string `yaml:"secret_key"`
	Bucket     string `yaml:"bucket"`
	PathPrefix string `yaml:"path_prefix"`
}

type RetentionConfig struct {
	Hours int `yaml:"hours"`
}

type EncryptionConfig struct {
	Password string `yaml:"password"`
}

type TelegramConfig struct {
	BotToken string `yaml:"bot_token"`
	ChatID   string `yaml:"chat_id"`
}

// LoadConfig loads the configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults if necessary
	if cfg.MySQL.Host == "" {
		cfg.MySQL.Host = "127.0.0.1"
	}
	if cfg.MySQL.Port == 0 {
		cfg.MySQL.Port = 3306
	}
	if cfg.LockFile == "" {
		cfg.LockFile = "/tmp/backup.lock"
	}
	if cfg.Retention.Hours == 0 {
		cfg.Retention.Hours = 24 * 7 // Default to 1 week
	}

	return &cfg, nil
}
