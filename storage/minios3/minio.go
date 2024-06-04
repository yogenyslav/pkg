// Package minios3 provides a wrapper around the MinIO Go SDK.
package minios3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

// S3 provides a wrapper around the MinIO Go SDK.
type S3 struct {
	cfg    *Config
	conn   *minio.Client
	tracer trace.Tracer
}

// MustNew creates a new S3 instance or panics if failed.
func MustNew(cfg *Config, tracer trace.Tracer) S3 {
	minioClient, err := minio.New(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Ssl,
	})
	if err != nil {
		log.Panic().Err(err).Msg("failed to create minio client")
	}

	return S3{
		cfg:    cfg,
		conn:   minioClient,
		tracer: tracer,
	}
}

// CreateBucket creates a new bucket with the given configuration.
func (s3 S3) CreateBucket(ctx context.Context, bucket *Bucket) error {
	return errors.Wrap(s3.conn.MakeBucket(ctx, bucket.Name, minio.MakeBucketOptions{
		Region: bucket.Region, ObjectLocking: bucket.Lock,
	}), "failed to create bucket")
}

// CreateBuckets creates all the buckets in the configuration.
func (s3 S3) CreateBuckets(ctx context.Context) error {
	for _, bucket := range s3.cfg.Buckets {
		exists, err := s3.BucketExists(ctx, bucket.Name)
		if err != nil {
			return errors.Wrap(err, "failed to check if bucket exists")
		}
		if !exists {
			err := s3.conn.MakeBucket(ctx, bucket.Name, minio.MakeBucketOptions{
				Region: bucket.Region, ObjectLocking: bucket.Lock,
			})
			if err != nil {
				return errors.Wrap(err, "failed to create bucket")
			}
		}
	}
	return nil
}

// ListBuckets lists all the buckets.
func (s3 S3) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	info, err := s3.conn.ListBuckets(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list buckets")
	}
	return info, nil
}

// BucketExists checks if the bucket exists.
func (s3 S3) BucketExists(ctx context.Context, name string) (bool, error) {
	exists, err := s3.conn.BucketExists(ctx, name)
	if err != nil {
		return false, errors.Wrap(err, "failed to check if bucket exists")
	}
	return exists, nil
}

// RemoveBucket removes the bucket.
func (s3 S3) RemoveBucket(ctx context.Context, name string) error {
	return errors.Wrap(s3.conn.RemoveBucket(ctx, name), "failed to remove bucket")
}

// ListObjects lists all the objects in the bucket.
func (s3 S3) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ctx, span := s3.tracer.Start(ctx, "S3.ListObjects")
	defer span.End()

	return s3.conn.ListObjects(ctx, bucket, opts)
}

// GetObject gets the object from the bucket.
func (s3 S3) GetObject(ctx context.Context, bucket, obj string, opts minio.GetObjectOptions) (*minio.Object, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.GetObject")
	defer span.End()

	objS3, err := s3.conn.GetObject(ctx, bucket, obj, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get object")
	}
	return objS3, nil
}

// PresignedGetObject returns a presigned URL for the object.
func (s3 S3) PresignedGetObject(ctx context.Context, bucket, obj string, exp time.Duration, params url.Values) (*url.URL, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.PresignedGetObject")
	defer span.End()

	objURL, err := s3.conn.PresignedGetObject(ctx, bucket, obj, exp, params)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get presigned object")
	}
	return objURL, nil
}

// PutObject puts the object in the bucket.
func (s3 S3) PutObject(ctx context.Context, bucket, obj string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.PutObject")
	defer span.End()

	info, err := s3.conn.PutObject(ctx, bucket, obj, reader, size, opts)
	if err != nil {
		return minio.UploadInfo{}, errors.Wrap(err, "failed to put object")
	}
	return info, nil
}

// RemoveObject removes the object from the bucket.
func (s3 S3) RemoveObject(ctx context.Context, bucket, obj string, opts minio.RemoveObjectOptions) error {
	ctx, span := s3.tracer.Start(ctx, "S3.RemoveObject")
	defer span.End()

	return errors.Wrap(s3.conn.RemoveObject(ctx, bucket, obj, opts), "failed to remove object")
}
