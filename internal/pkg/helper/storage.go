package helper

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/davexpro/backup/internal/config"
)

type Storage struct {
	client     *minio.Client
	bucket     string
	pathPrefix string
}

// NewStorage creates a new Storage instance using minio-go/v7.
func NewStorage(cfg config.R2Config) (*Storage, error) {
	// Remove scheme if present, minio-go expects host:port
	endpoint := cfg.Endpoint
	secure := true
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize minio client: %w", err)
	}

	return &Storage{
		client:     client,
		bucket:     cfg.Bucket,
		pathPrefix: cfg.PathPrefix,
	}, nil
}

// Upload uploads a file to storage.
func (s *Storage) Upload(ctx context.Context, filename string, content io.Reader) error {
	key := fmt.Sprintf("%s/%s", s.pathPrefix, filename)
	if s.pathPrefix == "" {
		key = filename
	}

	info, err := s.client.PutObject(ctx, s.bucket, key, content, -1, minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, err)
	}

	log.Printf("Uploaded %s to %s (Size: %d)", key, s.bucket, info.Size)
	return nil
}

// EnforceRetention deletes objects older than the specified retention period.
func (s *Storage) EnforceRetention(ctx context.Context, retentionHours int) error {
	if retentionHours <= 0 {
		return nil
	}

	retentionDuration := time.Duration(retentionHours) * time.Hour
	deadline := time.Now().Add(-retentionDuration)

	// List objects
	opts := minio.ListObjectsOptions{
		Prefix:    s.pathPrefix,
		Recursive: true,
	}

	deletedCount := 0
	for object := range s.client.ListObjects(ctx, s.bucket, opts) {
		if object.Err != nil {
			log.Printf("Error listing object: %v", object.Err)
			continue
		}

		if object.LastModified.Before(deadline) {
			err := s.client.RemoveObject(ctx, s.bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				log.Printf("Failed to delete expired object %s: %v", object.Key, err)
				continue
			}
			deletedCount++
			log.Printf("Deleted expired backup: %s (Time: %s)", object.Key, object.LastModified.Format(time.RFC3339))
		}
	}

	if deletedCount > 0 {
		log.Printf("Retention policy enforced: deleted %d expired backups.", deletedCount)
	}

	return nil
}
