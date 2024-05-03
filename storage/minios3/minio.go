package minios3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type S3 struct {
	cfg    *Config
	conn   *minio.Client
	tracer trace.Tracer
}

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

func (s3 S3) CreateBucket(ctx context.Context, bucket *Bucket) error {
	return s3.conn.MakeBucket(ctx, bucket.Name, minio.MakeBucketOptions{
		Region: bucket.Region, ObjectLocking: bucket.Lock,
	})
}

func (s3 S3) CreateBuckets(ctx context.Context) error {
	for _, bucket := range s3.cfg.Buckets {
		if exists, _ := s3.BucketExists(ctx, bucket.Name); !exists {
			err := s3.conn.MakeBucket(ctx, bucket.Name, minio.MakeBucketOptions{
				Region: bucket.Region, ObjectLocking: bucket.Lock,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s3 S3) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	return s3.conn.ListBuckets(ctx)
}

func (s3 S3) BucketExists(ctx context.Context, name string) (bool, error) {
	return s3.conn.BucketExists(ctx, name)
}

func (s3 S3) RemoveBucket(ctx context.Context, name string) error {
	return s3.conn.RemoveBucket(ctx, name)
}

func (s3 S3) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ctx, span := s3.tracer.Start(ctx, "S3.ListObjects")
	defer span.End()

	return s3.conn.ListObjects(ctx, bucket, opts)
}

func (s3 S3) GetObject(ctx context.Context, bucket, obj string, opts minio.GetObjectOptions) (*minio.Object, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.GetObject")
	defer span.End()

	return s3.conn.GetObject(ctx, bucket, obj, opts)
}

func (s3 S3) PresignedGetObject(ctx context.Context, bucket, obj string, exp time.Duration, params url.Values) (*url.URL, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.PresignedGetObject")
	defer span.End()

	return s3.conn.PresignedGetObject(ctx, bucket, obj, exp, params)
}

func (s3 S3) PutObject(ctx context.Context, bucket, obj string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	ctx, span := s3.tracer.Start(ctx, "S3.PutObject")
	defer span.End()

	return s3.conn.PutObject(ctx, bucket, obj, reader, size, opts)
}

func (s3 S3) RemoveObject(ctx context.Context, bucket, obj string, opts minio.RemoveObjectOptions) error {
	ctx, span := s3.tracer.Start(ctx, "S3.RemoveObject")
	defer span.End()

	return s3.conn.RemoveObject(ctx, bucket, obj, opts)
}
