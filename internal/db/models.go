package db

import (
	"time"
)

type BackupLog struct {
	ID        uint      `gorm:"primaryKey"`
	Database  string    `gorm:"size:255;index"`
	Status    string    `gorm:"size:20"` // SUCCESS, FAILED
	Size      int64     `gorm:"not null"`
	SHA256    string    `gorm:"size:64"`
	Error     string    `gorm:"type:text"`
	Duration  float64   `gorm:"comment:Duration in seconds"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (BackupLog) TableName() string {
	return "backup_logs"
}
