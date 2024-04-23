package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/trace"
)

type Database interface {
	InsertOne(ctx context.Context, coll string, doc interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	FindOne(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOneOptions) error
	FindMany(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOptions) error
	UpdateOne(ctx context.Context, coll string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateMany(ctx context.Context, coll string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	DeleteOne(ctx context.Context, coll string, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
}

type Mongo struct {
	mongo  *mongo.Client
	tracer trace.Tracer
}

func MustNew(cfg *Config, tracer trace.Tracer) Mongo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.RetryTimeout)*time.Second)
	defer cancel()

	url := fmt.Sprintf("mongodb://%s:%d/%s", cfg.Host, cfg.Port, cfg.Db)
	if cfg.AuthType != "" && cfg.AuthType != "no" {
		url = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Db)
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		log.Panic().Err(err)
	}

	return Mongo{
		mongo:  client,
		tracer: tracer,
	}
}

func (m Mongo) Close() {
	if err := m.mongo.Disconnect(context.Background()); err != nil {
		log.Err(err).Msg("failed to close mongo conn")
	}
}

func (m Mongo) InsertOne(ctx context.Context, coll string, doc interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	ctx, span := m.tracer.Start(ctx, "Mongo.InsertOne")
	defer span.End()

	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return m.mongo.Database("test").Collection(coll).InsertOne(ctx, data, opts...)
}

func (m Mongo) FindOne(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOneOptions) error {
	ctx, span := m.tracer.Start(ctx, "Mongo.FindOne")
	defer span.End()

	return m.mongo.Database("test").Collection(coll).FindOne(ctx, filter, opts...).Decode(dest)
}

func (m Mongo) FindMany(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOptions) error {
	ctx, span := m.tracer.Start(ctx, "Mongo.FindMany")
	defer span.End()

	cursor, err := m.mongo.Database("test").Collection(coll).Find(ctx, filter, opts...)
	if err != nil {
		return err
	}
	return cursor.All(ctx, dest)
}

func (m Mongo) UpdateOne(ctx context.Context, coll string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, span := m.tracer.Start(ctx, "Mongo.UpdateOne")
	defer span.End()

	return m.mongo.Database("test").Collection(coll).UpdateOne(ctx, filter, update, opts...)
}

func (m Mongo) UpdateMany(ctx context.Context, coll string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, span := m.tracer.Start(ctx, "Mongo.UpdateMany")
	defer span.End()

	return m.mongo.Database("test").Collection(coll).UpdateMany(ctx, filter, update, opts...)
}

func (m Mongo) DeleteOne(ctx context.Context, coll string, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	ctx, span := m.tracer.Start(ctx, "Mongo.DeleteOne")
	defer span.End()

	return m.mongo.Database("test").Collection(coll).DeleteOne(ctx, filter, opts...)
}
