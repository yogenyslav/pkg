// Package mongo provides a MongoDB client.
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Mongo provides a MongoDB client and tracing for operations.
type Mongo struct {
	mongo  *mongo.Client
	tracer trace.Tracer
	db     string
}

// New creates a new Mongo instance.
func New(cfg *Config, tracer trace.Tracer) (Mongo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.RetryTimeout)*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URL()))
	if err != nil {
		return Mongo{}, fmt.Errorf("failed to create mongo client: %w", err)
	}

	return Mongo{
		db:     cfg.DB,
		mongo:  client,
		tracer: tracer,
	}, nil
}

// Close closes the MongoDB client.
func (m Mongo) Close() error {
	if err := m.mongo.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("failed to close mongo conn: %w", err)
	}
	return nil
}

// InsertOne inserts a single document into the given collection.
func (m Mongo) InsertOne(ctx context.Context, coll string, doc interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.InsertOne", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document: %w", err)
	}

	res, err := m.mongo.Database(m.db).Collection(coll).InsertOne(ctx, data, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert one document: %w", err)
	}
	return res, nil
}

// FindOne finds a single document in the given collection.
func (m Mongo) FindOne(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOneOptions) error {
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.FindOne", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	if err := m.mongo.Database(m.db).Collection(coll).FindOne(ctx, filter, opts...).Decode(dest); err != nil {
		return fmt.Errorf("failed to find one document: %w", err)
	}
	return nil
}

// FindMany finds multiple documents in the given collection.
func (m Mongo) FindMany(ctx context.Context, coll string, filter, dest interface{}, opts ...*options.FindOptions) error {
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.FindMany", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	cursor, err := m.mongo.Database(m.db).Collection(coll).Find(ctx, filter, opts...)
	if err != nil {
		return fmt.Errorf("failed to find many documents: %w", err)
	}

	if err := cursor.All(ctx, dest); err != nil {
		return fmt.Errorf("failed to decode many documents: %w", err)
	}
	return nil
}

// UpdateOne updates a single document in the given collection.
func (m Mongo) UpdateOne(ctx context.Context, coll string, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) { //nolint:dupl // mongodb dictates the API
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.UpdateOne", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	res, err := m.mongo.Database(m.db).Collection(coll).UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to update one document: %w", err)
	}
	return res, nil
}

// UpdateMany updates multiple documents in the given collection.
func (m Mongo) UpdateMany(ctx context.Context, coll string, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) { //nolint:dupl // mongodb dictates the API
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.UpdateMany", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	res, err := m.mongo.Database(m.db).Collection(coll).UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to update many documents: %w", err)
	}
	return res, nil
}

// DeleteOne deletes a single document from the given collection.
func (m Mongo) DeleteOne(ctx context.Context, coll string, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	if m.tracer != nil {
		var span trace.Span
		ctx, span = m.tracer.Start(ctx, "Mongo.DeleteOne", trace.WithAttributes(
			attribute.String("collection", coll),
		))
		defer span.End()
	}

	res, err := m.mongo.Database(m.db).Collection(coll).DeleteOne(ctx, filter, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to delete one document: %w", err)
	}
	return res, nil
}
