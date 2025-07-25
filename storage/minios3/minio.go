// Package minios3 provides a wrapper around the MinIO Go SDK.
package minios3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ErrNewS3 is an error when failed to create new s3 client.
var ErrNewS3 = errors.New("failed to create new s3 client")

// S3 provides a wrapper around the MinIO Go SDK.
type S3 struct {
	cfg    *Config
	conn   *minio.Client
	tracer trace.Tracer
}

// New creates a new S3 instance or panics if failed.
func New(cfg *Config, tracer trace.Tracer, token string) (S3, error) {
	minioClient, err := minio.New(net.JoinHostPort(cfg.Host, cfg.Port), &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, token),
		Secure: cfg.Ssl,
	})
	if err != nil {
		return S3{}, errors.Join(ErrNewS3, err)
	}

	return S3{
		cfg:    cfg,
		conn:   minioClient,
		tracer: tracer,
	}, nil
}

// CreateBucket creates a new bucket with the given configuration.
func (s3 S3) CreateBucket(ctx context.Context, bucket *Bucket) error {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.CreateBucket", trace.WithAttributes(attribute.String(
			"bucket",
			bucket.Name,
		)))
		defer span.End()
	}

	return s3.conn.MakeBucket(ctx, bucket.Name, minio.MakeBucketOptions{
		Region: bucket.Region, ObjectLocking: bucket.Lock,
	})
}

// CreateBuckets creates all the buckets in the configuration.
func (s3 S3) CreateBuckets(ctx context.Context) error {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.CreateBuckets")
		defer span.End()
	}

	for _, bucket := range s3.cfg.Buckets {
		exists, err := s3.BucketExists(ctx, bucket.Name)
		if err != nil {
			return fmt.Errorf("failed to check if bucket exists: %w", err)
		}
		if !exists {
			err = s3.CreateBucket(ctx, &Bucket{
				Name:   bucket.Name,
				Region: bucket.Region,
				Lock:   bucket.Lock,
			})
			if err != nil {
				return fmt.Errorf("failed to create bucket: %w", err)
			}
		}
	}
	return nil
}

// ListBuckets lists all the buckets.
func (s3 S3) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.ListBuckets")
		defer span.End()
	}

	info, err := s3.conn.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}
	return info, nil
}

// BucketExists checks if the bucket exists.
func (s3 S3) BucketExists(ctx context.Context, name string) (bool, error) {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.BucketExists", trace.WithAttributes(attribute.String(
			"bucket",
			name,
		)))
		defer span.End()
	}

	exists, err := s3.conn.BucketExists(ctx, name)
	if err != nil {
		return false, fmt.Errorf("failed to check if bucket exists: %w", err)
	}
	return exists, nil
}

// RemoveBucket removes the bucket.
func (s3 S3) RemoveBucket(ctx context.Context, name string) error {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.RemoveBucket", trace.WithAttributes(attribute.String(
			"bucket",
			name,
		)))
		defer span.End()
	}

	if err := s3.conn.RemoveBucket(ctx, name); err != nil {
		return fmt.Errorf("failed to remove bucket: %w", err)
	}
	return nil
}

// ListObjects lists all the objects in the bucket.
func (s3 S3) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.ListObjects", trace.WithAttributes(attribute.String(
			"bucket",
			bucket,
		)))
		defer span.End()
	}

	return s3.conn.ListObjects(ctx, bucket, opts)
}

// GetObject gets the object from the bucket.
func (s3 S3) GetObject(ctx context.Context, bucket, obj string, opts minio.GetObjectOptions) (*minio.Object, error) {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.GetObject", trace.WithAttributes(
			attribute.String(
				"bucket",
				bucket,
			),
			attribute.String(
				"object",
				obj,
			),
		))
		defer span.End()
	}

	objS3, err := s3.conn.GetObject(ctx, bucket, obj, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	return objS3, nil
}

// PresignedGetObject returns a presigned URL for the object.
func (s3 S3) PresignedGetObject(
	ctx context.Context,
	bucket, obj string,
	exp time.Duration,
	params url.Values,
) (*url.URL, error) {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.PresignedGetObject", trace.WithAttributes(
			attribute.String(
				"bucket",
				bucket,
			),
			attribute.String(
				"object",
				obj,
			),
		))
		defer span.End()
	}

	objURL, err := s3.conn.PresignedGetObject(ctx, bucket, obj, exp, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get presigned object: %w", err)
	}
	return objURL, nil
}

// PutObject puts the object in the bucket.
func (s3 S3) PutObject(
	ctx context.Context,
	bucket, obj string,
	reader io.Reader,
	size int64,
	opts minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.PutObject", trace.WithAttributes(
			attribute.String(
				"bucket",
				bucket,
			),
			attribute.String(
				"object",
				obj,
			),
		))
		defer span.End()
	}

	info, err := s3.conn.PutObject(ctx, bucket, obj, reader, size, opts)
	if err != nil {
		return minio.UploadInfo{}, fmt.Errorf("failed to put object: %w", err)
	}
	return info, nil
}

// RemoveObject removes the object from the bucket.
func (s3 S3) RemoveObject(ctx context.Context, bucket, obj string, opts minio.RemoveObjectOptions) error {
	if s3.tracer != nil {
		var span trace.Span
		ctx, span = s3.tracer.Start(ctx, "S3.RemoveObject", trace.WithAttributes(
			attribute.String(
				"bucket",
				bucket,
			),
			attribute.String(
				"object",
				obj,
			),
		))
		defer span.End()
	}

	if err := s3.conn.RemoveObject(ctx, bucket, obj, opts); err != nil {
		return fmt.Errorf("failed to remove object: %w", err)
	}
	return nil
}
