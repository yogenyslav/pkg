// Package rediscache provides a simple wrapper around the go-redis library to interact with a Redis cache.
package rediscache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ErrNotFound reports that key doesn't exist.
var ErrNotFound = errors.New("key not found")

// Redis wraps go-redis client and adds tracer to all operations.
type Redis struct {
	rc     *redis.Client
	tracer trace.Tracer
}

// New creates a new Redis instance.
func New(cfg *Config, tracer trace.Tracer) (Redis, error) {
	opts := redis.Options{
		Addr:     net.JoinHostPort(cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(&opts)

	if err := client.Ping(context.Background()).Err(); err != nil {
		return Redis{}, fmt.Errorf("failed to create redis client: %w", err)
	}
	return Redis{rc: client, tracer: tracer}, nil
}

// SetStruct sets a struct in the cache with the given key and expiration time.
func (r Redis) SetStruct(ctx context.Context, k string, v any, exp time.Duration) error {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.SetStruct",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal struct: %w", err)
	}

	if err = r.rc.Set(ctx, k, data, exp).Err(); err != nil {
		return fmt.Errorf("failed to set struct: %w", err)
	}
	return nil
}

// SetPrimitive sets a primitive in the cache with the given key and expiration time.
func (r Redis) SetPrimitive(ctx context.Context, k string, v any, exp time.Duration) error {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.SetPrimitive",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	if err := r.rc.Set(ctx, k, v, exp).Err(); err != nil {
		return fmt.Errorf("failed to set primitive: %w", err)
	}
	return nil
}

// GetStruct gets a struct from the cache with the given key.
func (r Redis) GetStruct(ctx context.Context, dest any, k string) error {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetStruct",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Bytes()
	if errors.Is(err, redis.Nil) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to get struct: %w", err)
	}

	if err = json.Unmarshal(res, dest); err != nil {
		return fmt.Errorf("failed to unmarshal struct: %w", err)
	}
	return nil
}

// GetString gets a string from the cache with the given key.
func (r Redis) GetString(ctx context.Context, k string) (string, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetString",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrNotFound
	}
	return res, fmt.Errorf("failed to get string: %w", err)
}

// GetInt gets an integer from the cache with the given key.
func (r Redis) GetInt(ctx context.Context, k string) (int, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetInt",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Int()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	return res, fmt.Errorf("failed to get int: %w", err)
}

// GetInt64 gets an int64 from the cache with the given key.
func (r Redis) GetInt64(ctx context.Context, k string) (int64, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetInt64",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	return res, fmt.Errorf("failed to get int64: %w", err)
}

// GetFloat gets a float64 from the cache with the given key.
func (r Redis) GetFloat(ctx context.Context, k string) (float64, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetFloat",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Float64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	return res, fmt.Errorf("failed to get float64: %w", err)
}

// GetBool gets a bool from the cache with the given key.
func (r Redis) GetBool(ctx context.Context, k string) (bool, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetBool",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Bool()
	if errors.Is(err, redis.Nil) {
		return false, ErrNotFound
	}
	return res, fmt.Errorf("failed to get bool: %w", err)
}

// GetBytes gets a byte slice from the cache with the given key.
func (r Redis) GetBytes(ctx context.Context, k string) ([]byte, error) {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.GetBytes",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	res, err := r.rc.Get(ctx, k).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	return res, fmt.Errorf("failed to get bytes: %w", err)
}

// Del deletes a key from the cache.
func (r Redis) Del(ctx context.Context, k string) error {
	if r.tracer != nil {
		var span trace.Span
		ctx, span = r.tracer.Start(
			ctx,
			"Redis.Del",
			trace.WithAttributes(attribute.String("key", k)),
		)
		defer span.End()
	}

	err := r.rc.Del(ctx, k).Err()
	if errors.Is(err, redis.Nil) {
		return ErrNotFound
	}
	return fmt.Errorf("failed to delete key: %w", err)
}
