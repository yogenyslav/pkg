//go:generate mockgen -source=./postgres.go -destination=./mock/postgres.go -package=mock

package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

const txKey = "tx"

var (
	ErrTimeoutExceeded = errors.New("postgres connection timeout exceeded")
)

type Database interface {
	BeginSerializable(ctx context.Context) (context.Context, error)
	Query(ctx context.Context, dest any, query string, args ...any) error
	QuerySlice(ctx context.Context, dest any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	QueryTx(ctx context.Context, dest any, query string, args ...any) error
	QuerySliceTx(ctx context.Context, dest any, query string, args ...any) error
	ExecTx(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

type Postgres struct {
	pool   *pgxpool.Pool
	tracer trace.Tracer
}

func MustNew(cfg *Config, tracer trace.Tracer) Postgres {
	var (
		err         error
		pool        *pgxpool.Pool
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.RetryTimeout)*time.Second)
		url         = "postgres://%s:%s@%s:%d/%s?sslmode="
	)
	defer cancel()

	if cfg.Ssl {
		url += "enable"
	} else {
		url += "disable"
	}
	connString := fmt.Sprintf(url, cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Db)

	for ctx.Err() == nil {
		time.Sleep(time.Second)
		pool, err = pgxpool.New(ctx, connString)
		if err != nil {
			log.Err(err).Msg("failed to open new pool: %v")
			continue
		}

		if err = pool.Ping(ctx); err != nil {
			log.Err(err).Msg("can't access postgres: %v")
			continue
		}

		return Postgres{pool: pool, tracer: tracer}
	}
	log.Panic().Err(ErrTimeoutExceeded)
	return Postgres{}
}

func (pg Postgres) GetPool() *pgxpool.Pool {
	return pg.pool
}

func (pg Postgres) Close() {
	pg.pool.Close()
}

func (pg Postgres) BeginSerializable(ctx context.Context) (context.Context, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.BeginSerializable")
	defer span.End()

	tx, err := pg.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, txKey, tx)
	return ctx, nil
}

func (pg Postgres) Query(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.Query")
	defer span.End()
	return pgxscan.Get(ctx, pg.pool, dest, query, args...)
}

func (pg Postgres) QuerySlice(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.QuerySlice")
	defer span.End()
	return pgxscan.Select(ctx, pg.pool, dest, query, args...)
}

func (pg Postgres) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.Exec")
	defer span.End()
	return pg.pool.Exec(ctx, query, args...)
}

func (pg Postgres) QueryTx(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.QueryTx")
	defer span.End()

	tx := ctx.Value(txKey).(pgx.Tx)
	return pgxscan.Get(ctx, tx, dest, query, args...)
}

func (pg Postgres) QuerySliceTx(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.QuerySliceTx")
	defer span.End()

	tx := ctx.Value(txKey).(pgx.Tx)
	return pgxscan.Select(ctx, tx, dest, query, args...)
}

func (pg Postgres) ExecTx(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.ExecTx")
	defer span.End()

	tx := ctx.Value(txKey).(pgx.Tx)
	return tx.Exec(ctx, query, args...)
}
