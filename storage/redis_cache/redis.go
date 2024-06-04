// Package rediscache provides a simple wrapper around the go-redis library to interact with a Redis cache.
package rediscache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

// Redis wraps go-redis client and adds tracer to all operations.
type Redis struct {
	rc     *redis.Client
	tracer trace.Tracer
}

// MustNew creates a new Redis instance or panics if failed.
func MustNew(cfg *Config, tracer trace.Tracer) Redis {
	opts := redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := redis.NewClient(&opts)

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Panic().Err(err).Msg("failed to create redis client")
	}
	return Redis{rc: client, tracer: tracer}
}

// SetStruct sets a struct in the cache with the given key and expiration time.
func (r Redis) SetStruct(ctx context.Context, k string, v any, exp time.Duration) error {
	ctx, span := r.tracer.Start(ctx, "Redis.SetStruct")
	defer span.End()

	data, err := json.Marshal(v)
	if err != nil {
		return errors.Wrap(err, "failed to marshal struct")
	}
	return errors.Wrap(r.rc.Set(ctx, k, data, exp).Err(), "failed to set struct")
}

// SetPrimitive sets a primitive in the cache with the given key and expiration time.
func (r Redis) SetPrimitive(ctx context.Context, k string, v any, exp time.Duration) error {
	ctx, span := r.tracer.Start(ctx, "Redis.SetPrimitive")
	defer span.End()

	return errors.Wrap(r.rc.Set(ctx, k, v, exp).Err(), "failed to set primitive")
}

// GetStruct gets a struct from the cache with the given key.
func (r Redis) GetStruct(ctx context.Context, dest any, k string) error {
	ctx, span := r.tracer.Start(ctx, "Redis.GetStruct")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Bytes()
	if err != nil {
		return errors.Wrap(err, "failed to get struct")
	}
	err = json.Unmarshal(res, dest)
	return errors.Wrap(err, "failed to unmarshal struct")
}

// GetString gets a string from the cache with the given key.
func (r Redis) GetString(ctx context.Context, k string) (string, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetString")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Result()
	return res, errors.Wrap(err, "failed to get string")
}

// GetInt gets an integer from the cache with the given key.
func (r Redis) GetInt(ctx context.Context, k string) (int, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetInt")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Int()
	return res, errors.Wrap(err, "failed to get int")
}

// GetInt64 gets an int64 from the cache with the given key.
func (r Redis) GetInt64(ctx context.Context, k string) (int64, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetInt64")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Int64()
	return res, errors.Wrap(err, "failed to get int64")
}

// GetFloat gets a float64 from the cache with the given key.
func (r Redis) GetFloat(ctx context.Context, k string) (float64, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetFloat")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Float64()
	return res, errors.Wrap(err, "failed to get float64")
}

// GetBool gets a bool from the cache with the given key.
func (r Redis) GetBool(ctx context.Context, k string) (bool, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetBool")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Bool()
	return res, errors.Wrap(err, "failed to get bool")
}

// GetBytes gets a byte slice from the cache with the given key.
func (r Redis) GetBytes(ctx context.Context, k string) ([]byte, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetBytes")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Bytes()
	return res, errors.Wrap(err, "failed to get bytes")
}

// Del deletes a key from the cache.
func (r Redis) Del(ctx context.Context, k string) error {
	ctx, span := r.tracer.Start(ctx, "Redis.Del")
	defer span.End()

	return errors.Wrap(r.rc.Del(ctx, k).Err(), "failed to delete key")
}
