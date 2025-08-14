package nats

import (
	"context"
	"fmt"
	"time"

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
	messageID := getMessageID(ctx, headers)

	_, span := n.trace(
		ctx,
		"NATS publish",
		attribute.String("messageID", messageID),
		attribute.String("subj", subj),
	)
	defer span.End()

	data, err := proto.Marshal(&Message{
		Ts:      timestamppb.Now(),
		Id:      messageID,
		TraceId: span.SpanContext().TraceID().String(),
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("marshal proto message: %w", err)
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
		return fmt.Errorf("%s: %w", desc, err)
	}

	span.AddEvent("published to nats")

	return nil
}

// Request sends a request to nats and waits for a response.
func (n *Nats) Request(
	ctx context.Context,
	subj string,
	payload []byte,
	headers map[string]string,
	timeout time.Duration,
) (*nats.Msg, error) {
	messageID := getMessageID(ctx, headers)

	_, span := n.trace(
		ctx,
		"NATS request",
		attribute.String("messageID", messageID),
		attribute.String("subj", subj),
	)
	defer span.End()

	data, err := proto.Marshal(&Message{
		Ts:      timestamppb.Now(),
		Id:      messageID,
		TraceId: span.SpanContext().TraceID().String(),
		Payload: payload,
	})
	if err != nil {
		desc := "marshal proto message"
		span.AddEvent(desc)
		span.RecordError(err)
		span.SetStatus(codes.Error, desc)
		return nil, fmt.Errorf("%s: %w", desc, err)
	}

	natsMsg := nats.NewMsg(subj)
	natsMsg.Data = data
	for header, value := range headers {
		natsMsg.Header.Add(header, value)
	}

	resp, err := n.conn.RequestMsg(natsMsg, timeout)
	if err != nil {
		desc := "error requesting from nats"
		span.AddEvent(desc)
		span.RecordError(err)
		span.SetStatus(codes.Error, desc)
		return nil, fmt.Errorf("%s: %w", desc, err)
	}

	return resp, nil
}

// Subscribe subscribes to a subject and sends messages to the handler.
func (n *Nats) Subscribe(
	subj string,
	handler func(ctx context.Context, msg *Message) ([]byte, error),
) (*nats.Subscription, error) {
	sub, err := n.conn.Subscribe(subj, func(m *nats.Msg) {
		ctx, span := n.trace(context.Background(), "NATS pub/sub response", attribute.String("subj", subj))
		defer span.End()

		var msg Message
		if err := proto.Unmarshal(m.Data, &msg); err != nil {
			desc := "unmarshal proto message"
			span.AddEvent(desc)
			span.RecordError(err)
			span.SetStatus(codes.Error, desc)
			return
		}

		span.SetAttributes(attribute.String("messageID", msg.Id))

		result, err := handler(ctx, &msg)
		if err != nil {
			desc := "handler returned an error"
			span.AddEvent(desc)
			span.RecordError(err)
			span.SetStatus(codes.Error, desc)
			return
		}

		resp := &Message{
			Ts:      timestamppb.Now(),
			Id:      uuid.NewString(),
			TraceId: msg.TraceId,
			Payload: result,
		}
		data, err := proto.Marshal(resp)
		if err != nil {
			desc := "marshal proto response message"
			span.AddEvent(desc)
			span.RecordError(err)
			span.SetStatus(codes.Error, desc)
			return
		}

		if err = m.Respond(data); err != nil {
			desc := "respond to nats message"
			span.AddEvent(desc)
			span.RecordError(err)
			span.SetStatus(codes.Error, desc)
			return
		}
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe to subject %s: %w", subj, err)
	}

	return sub, nil
}
