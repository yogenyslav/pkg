package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrJetStreamNotEnabled is an error when JetStream methods were used before it was initialized.
var ErrJetStreamNotEnabled = errors.New(
	"jetstream is not enabled for this client, use JetStream() method to turn it on",
)

// JetStream enables jetstream for nats.
func (n *Nats) JetStream() (jetstream.JetStream, error) {
	if n.stream != nil {
		return n.stream, nil
	}

	stream, err := jetstream.New(n.conn)
	if err != nil {
		return nil, fmt.Errorf("connect to jetstream: %v", err)
	}

	n.stream = stream
	n.router = &router{}

	return n.stream, nil
}

// PublishSync publishes raw data into jetstream (waits for ack).
func (n *Nats) PublishSync(ctx context.Context, subj, reply string, payload []byte, headers map[string]string) error {
	if n.stream == nil {
		return ErrJetStreamNotEnabled
	}

	ctx, span := n.trace(ctx, "JetStream publish", attribute.String("subj", subj))
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
	if reply != "" {
		natsMsg.Reply = reply
	}
	for header, value := range headers {
		natsMsg.Header.Add(header, value)
	}

	ack, err := n.stream.PublishMsg(ctx, natsMsg)
	if err != nil {
		desc := "error publishing to stream"
		span.AddEvent(desc)
		span.RecordError(err)
		span.SetStatus(codes.Error, desc)
		return fmt.Errorf("%s: %v", desc, err)
	}

	span.AddEvent(
		"published to stream",
		trace.WithAttributes(attribute.String("stream", ack.Stream), attribute.String("domain", ack.Domain)),
	)
	return nil
}

// PublishAsync publishes raw data into jetstream (doesn't wait for ack).
func (n *Nats) PublishAsync(
	ctx context.Context,
	subj, reply string,
	payload []byte,
	withAck bool,
	headers map[string]string,
) error {
	if n.stream == nil {
		return ErrJetStreamNotEnabled
	}

	_, span := n.trace(ctx, "JetStream async publish", attribute.String("subj", subj))
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
	if reply != "" {
		natsMsg.Reply = reply
	}
	for header, value := range headers {
		natsMsg.Header.Add(header, value)
	}

	ackFuture, err := n.stream.PublishMsgAsync(natsMsg)
	if err != nil {
		desc := "error async publishing to stream"
		span.AddEvent(desc)
		span.RecordError(err)
		span.SetStatus(codes.Error, desc)
		return fmt.Errorf("%s: %v", desc, err)
	}

	if withAck {
		select {
		case ack := <-ackFuture.Ok():
			span.AddEvent(
				"async published to stream",
				trace.WithAttributes(attribute.String("stream", ack.Stream), attribute.String("domain", ack.Domain)),
			)
		case <-ctx.Done():
			return ErrAckTimeout
		}
	}

	return nil
}

// Stream creates or updates stream by name.
func (n *Nats) Stream(ctx context.Context, cfg StreamConfig) (jetstream.Stream, error) {
	if n.stream == nil {
		return nil, ErrJetStreamNotEnabled
	}

	streamList := n.stream.ListStreams(ctx)
	subjects := make([]string, len(cfg.Subjects))
	copy(subjects, cfg.Subjects)

	for info := range streamList.Info() {
		if info.Config.Name == cfg.Name {
			subjects = append(subjects, info.Config.Subjects...)
		}
	}

	subjectsSet := make(map[string]struct{}, len(subjects))
	for _, subj := range subjects {
		subjectsSet[subj] = struct{}{}
	}

	uniqueSubjects := make([]string, 0, len(subjectsSet))
	for subj := range subjectsSet {
		uniqueSubjects = append(uniqueSubjects, subj)
	}

	if err := streamList.Err(); err != nil {
		return nil, fmt.Errorf("list existing streams: %v", err)
	}

	stream, err := n.stream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        cfg.Name,
		Subjects:    uniqueSubjects,
		Retention:   cfg.RetentionPolicy,
		MaxAge:      time.Second * time.Duration(cfg.MaxAgeSec),
		Replicas:    cfg.Replicas,
		Compression: cfg.Compression,
	})
	if err != nil {
		return nil, fmt.Errorf("create new stream: %v", err)
	}

	return stream, nil
}

// Consumer creates new durable consumer with specified filters.
func (n *Nats) Consumer(ctx context.Context, cfg ConsumerConfig) (jetstream.Consumer, error) {
	if n.stream == nil {
		return nil, ErrJetStreamNotEnabled
	}

	cons, err := n.stream.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:        cfg.ConsumerName,
		FilterSubjects: cfg.Filters,
		AckPolicy:      cfg.AckPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create jetstream consumer: %v", err)
	}

	return cons, nil
}

// AddConsumer adds jetstream consumer to the list of active consumers.
func (n *Nats) AddConsumer(cons jetstream.Consumer) {
	if n.stream == nil {
		panic(ErrJetStreamNotEnabled)
	}

	if n.consumers == nil {
		n.consumers = make([]jetstream.Consumer, 0)
	}
	n.consumers = append(n.consumers, cons)
}

// ActiveConsumers returns the list of active consumers.
func (n *Nats) ActiveConsumers() []jetstream.Consumer {
	if n.stream == nil {
		panic(ErrJetStreamNotEnabled)
	}
	return n.consumers
}

// ProcessStream starts consuming messages with cons, applying handler to each of them.
func (n *Nats) ProcessStream(
	ctx context.Context,
	cons jetstream.Consumer,
	handler jetstream.MessageHandler,
	errHandler jetstream.ConsumeErrHandlerFunc,
) (jetstream.ConsumeContext, error) {
	if n.stream == nil {
		return nil, ErrJetStreamNotEnabled
	}

	msgs, err := cons.Consume(handler, jetstream.ConsumeErrHandler(errHandler))
	if err != nil {
		return nil, fmt.Errorf("start consuming messages from stream: %v", err)
	}

	return msgs, err
}

// ConsumerMessageHandler returns default message handler for stream consumer.
func (n *Nats) ConsumerMessageHandler(ctx context.Context) jetstream.MessageHandler {
	return func(natsMsg jetstream.Msg) {
		ctx, span := n.trace(ctx, "JetStream consume message")
		defer span.End()

		ctx = loggerCtx(ctx)
		l := log.Ctx(ctx)

		subj := natsMsg.Subject()
		l.Debug().Str("subj", subj).Msg("got message")

		err := natsMsg.DoubleAck(ctx)
		if err != nil {
			l.Err(err).Msg("double ack the message")
			return
		}
		l.Debug().Str("subj", natsMsg.Subject()).Msg("ack")

		var msg Message
		if err = proto.Unmarshal(natsMsg.Data(), &msg); err != nil {
			l.Err(err).Msg("unmarshal message")
			return
		}

		if err = n.router.processStreamMessage(ctx, natsMsg.Subject(), &msg); err != nil {
			l.Err(err).Msg("process message from stream")
			return
		}

		l.Info().Str("subj", natsMsg.Subject()).Msg("message processed successfuly")
	}
}

// ConsumerErrHandler returns default error handler for stream consumer.
func (n *Nats) ConsumerErrHandler(ctx context.Context) jetstream.ConsumeErrHandlerFunc {
	return func(consumeCtx jetstream.ConsumeContext, err error) {
		_, span := n.trace(ctx, "JetStream consume error")
		defer span.End()

		log.Err(err).Msg("consume jetstream message")

		desc := "consume error"
		span.AddEvent(desc)
		span.SetStatus(codes.Error, desc)
		span.RecordError(err)
	}
}

// Handle registers new [StreamEventHandler] to handle messages with specified subject.
func (n *Nats) Handle(subj string, h StreamEventHandler) {
	if n.stream == nil {
		panic(ErrJetStreamNotEnabled)
	}

	n.router.streamHandlers.Store(subj, h)
}
