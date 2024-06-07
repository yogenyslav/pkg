// Package postgres provides a simple wrapper around the pgx library to interact with a PostgreSQL database.
package postgres

import (
	"context"
	"time"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type ctxKey string

const (
	txKey ctxKey = "tx"
)

// Postgres wraps pgxpool.Pool and adds tracer to all operations.
type Postgres struct {
	pool   *pgxpool.Pool
	tracer trace.Tracer
}

// MustNew creates a new Postgres instance or panics if failed.
func MustNew(cfg *Config, tracer trace.Tracer) Postgres {
	var (
		err         error
		pool        *pgxpool.Pool
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.RetryTimeout)*time.Second)
	)
	defer cancel()

	for ctx.Err() == nil {
		time.Sleep(time.Second)
		pool, err = pgxpool.New(ctx, cfg.URL())
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
	log.Panic().Msg("failed to connect to postgres")
	return Postgres{}
}

// GetPool returns the underlying pgxpool.Pool.
func (pg Postgres) GetPool() *pgxpool.Pool {
	return pg.pool
}

// Close closes the underlying db connection.
func (pg Postgres) Close() {
	pg.pool.Close()
}

// BeginSerializable starts a new transaction with serializable isolation level.
func (pg Postgres) BeginSerializable(ctx context.Context) (context.Context, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.BeginSerializable")
	defer span.End()

	tx, err := pg.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return nil, errors.Unwrap(err)
	}

	ctx = context.WithValue(ctx, txKey, tx)
	return ctx, nil
}

// CommitTx commits the transaction.
func (pg Postgres) CommitTx(ctx context.Context) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.CommitTx")
	defer span.End()

	tx, ok := ctx.Value(txKey).(pgx.Tx)
	if !ok {
		return errors.New("transaction not found in context")
	}

	return errors.Wrap(tx.Commit(ctx), "failed to commit transaction")
}

// RollbackTx rolls back the transaction.
func (pg Postgres) RollbackTx(ctx context.Context) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.RollbackTx")
	defer span.End()

	tx, ok := ctx.Value(txKey).(pgx.Tx)
	if !ok {
		return errors.New("transaction not found in context")
	}

	return errors.Wrap(tx.Rollback(ctx), "failed to rollback transaction")
}

// Query executes a query that returns a single row.
func (pg Postgres) Query(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.Query")
	defer span.End()
	return errors.Wrap(pgxscan.Get(ctx, pg.pool, dest, query, args...), "failed to get row")
}

// QuerySlice executes a query that returns multiple rows.
func (pg Postgres) QuerySlice(ctx context.Context, dest any, query string, args ...any) error {
	ctx, span := pg.tracer.Start(ctx, "Postgres.QuerySlice")
	defer span.End()
	return errors.Wrap(pgxscan.Select(ctx, pg.pool, dest, query, args...), "failed to get rows")
}

// Exec executes a query that doesn't return any rows.
func (pg Postgres) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.Exec")
	defer span.End()

	tag, err := pg.pool.Exec(ctx, query, args...)
	return tag, errors.Wrap(err, "failed to exec")
}

// QueryTx executes a query that returns a single row in a transaction.
func (pg Postgres) QueryTx(ctx context.Context, dest any, query string, args ...any) error { //nolint:dupl // because of pgx api
	ctx, span := pg.tracer.Start(ctx, "Postgres.QueryTx")
	defer span.End()

	tx, ok := ctx.Value(txKey).(pgx.Tx)
	if !ok {
		return errors.New("transaction not found in context")
	}

	return errors.Wrap(pgxscan.Get(ctx, tx, dest, query, args...), "failed to get row in transaction")
}

// QuerySliceTx executes a query that returns multiple rows in a transaction.
func (pg Postgres) QuerySliceTx(ctx context.Context, dest any, query string, args ...any) error { //nolint:dupl // because of pgx api
	ctx, span := pg.tracer.Start(ctx, "Postgres.QuerySliceTx")
	defer span.End()

	tx, ok := ctx.Value(txKey).(pgx.Tx)
	if !ok {
		return errors.New("transaction not found in context")
	}
	return errors.Wrap(pgxscan.Select(ctx, tx, dest, query, args...), "failed to get rows in transaction")
}

// ExecTx executes a query that doesn't return any rows in a transaction.
func (pg Postgres) ExecTx(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	ctx, span := pg.tracer.Start(ctx, "Postgres.ExecTx")
	defer span.End()

	tx, ok := ctx.Value(txKey).(pgx.Tx)
	if !ok {
		return pgconn.CommandTag{}, errors.New("transaction not found in context")
	}
	tag, err := tx.Exec(ctx, query, args...)
	return tag, errors.Wrap(err, "failed to exec in transaction")
}
