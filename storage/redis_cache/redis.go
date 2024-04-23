package rediscache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type Cache interface {
	SetStruct(ctx context.Context, k string, v any, exp time.Duration) error
	SetPrimitive(ctx context.Context, k string, v any, exp time.Duration) error
	GetStruct(ctx context.Context, dest any, k string) error
	GetString(ctx context.Context, k string) (string, error)
	GetInt(ctx context.Context, k string) (int, error)
	GetInt64(ctx context.Context, k string) (int64, error)
	GetFloat(ctx context.Context, k string) (float64, error)
	GetBool(ctx context.Context, k string) (bool, error)
	GetBytes(ctx context.Context, k string) ([]byte, error)
	Del(ctx context.Context, k string) error
}

type Redis struct {
	rc     *redis.Client
	tracer trace.Tracer
}

func MustNew(cfg *Config, tracer trace.Tracer) Redis {
	opts := redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.Db,
	}

	client := redis.NewClient(&opts)

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Panic().Err(err)
	}
	return Redis{rc: client, tracer: tracer}
}

func (r Redis) SetStruct(ctx context.Context, k string, v any, exp time.Duration) error {
	ctx, span := r.tracer.Start(ctx, "Redis.SetStruct")
	defer span.End()

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.rc.Set(ctx, k, data, exp).Err()
}

func (r Redis) SetPrimitive(ctx context.Context, k string, v any, exp time.Duration) error {
	ctx, span := r.tracer.Start(ctx, "Redis.SetPrimitive")
	defer span.End()

	return r.rc.Set(ctx, k, v, exp).Err()
}

func (r Redis) GetStruct(ctx context.Context, dest any, k string) error {
	ctx, span := r.tracer.Start(ctx, "Redis.GetStruct")
	defer span.End()

	res, err := r.rc.Get(ctx, k).Bytes()
	if err != nil {
		return err
	}
	err = json.Unmarshal(res, dest)
	return err
}

func (r Redis) GetString(ctx context.Context, k string) (string, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetString")
	defer span.End()

	return r.rc.Get(ctx, k).Result()
}

func (r Redis) GetInt(ctx context.Context, k string) (int, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetInt")
	defer span.End()

	return r.rc.Get(ctx, k).Int()
}

func (r Redis) GetInt64(ctx context.Context, k string) (int64, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetInt64")
	defer span.End()

	return r.rc.Get(ctx, k).Int64()
}

func (r Redis) GetFloat(ctx context.Context, k string) (float64, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetFloat")
	defer span.End()

	return r.rc.Get(ctx, k).Float64()
}

func (r Redis) GetBool(ctx context.Context, k string) (bool, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetBool")
	defer span.End()

	return r.rc.Get(ctx, k).Bool()
}

func (r Redis) GetBytes(ctx context.Context, k string) ([]byte, error) {
	ctx, span := r.tracer.Start(ctx, "Redis.GetBytes")
	defer span.End()

	return r.rc.Get(ctx, k).Bytes()
}

func (r Redis) Del(ctx context.Context, k string) error {
	ctx, span := r.tracer.Start(ctx, "Redis.Del")
	defer span.End()

	return r.rc.Del(ctx, k).Err()
}
