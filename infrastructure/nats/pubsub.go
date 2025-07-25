package nats

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Conn returns underlying connection to nats.
func (n *Nats) Conn() *nats.Conn {
	return n.conn
}

// PublishNats publishes basic message into nats.
func (n *Nats) PublishNats(ctx context.Context, subj string, payload []byte, headers map[string]string) error {
	_, span := n.trace(ctx, "NATS publish", attribute.String("subj", subj))
	defer span.End()

	data, err := proto.Marshal(&Message{
		Ts:      timestamppb.Now(),
		Id:      uuid.NewString(),
		TraceId: span.SpanContext().TraceID().String(),
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("marshal proto message: %v", err)
	}

	natsMsg := nats.NewMsg(subj)
	natsMsg.Data = data
	for header, value := range headers {
		natsMsg.Header.Add(header, value)
	}

	err = n.conn.PublishMsg(natsMsg)
	if err != nil {
		desc := "error publishing to nats"
		span.AddEvent(desc)
		span.RecordError(err)
		span.SetStatus(codes.Error, desc)
		return fmt.Errorf("%s: %v", desc, err)
	}

	span.AddEvent("published to nats")

	return nil
}

// TODO: implement more methods for pub/sub pattern.
