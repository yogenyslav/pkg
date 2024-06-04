package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

var (
	// ErrGetPartitions is returned when failed to get partitions.
	ErrGetPartitions = errors.New("failed to get partitions")

	// ErrConsumePartition is returned when failed to consume partition.
	ErrConsumePartition = errors.New("error consuming partition")
)

// Consumer is a Kafka consumer.
type Consumer struct {
	Config         *Config
	SingleConsumer sarama.Consumer
}

// MustNewConsumer creates a new Kafka consumer or panics if failed.
// config is the Kafka configuration.
// commitInterval is the interval for the consumer to commit the offset.
func MustNewConsumer(config *Config, commitInterval time.Duration) *Consumer {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = false
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = commitInterval

	if config.OffsetNewest {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	brokers := make([]string, len(config.Brokers))
	for idx, broker := range config.Brokers {
		brokers[idx] = fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	}

	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		log.Panic().Err(err).Msg("failed to create new consumer")
	}

	return &Consumer{
		Config:         config,
		SingleConsumer: consumer,
	}
}

// Subscribe subscribes to a Kafka topic and sends messages to the out channel in a separate goroutine.
func (consumer *Consumer) Subscribe(ctx context.Context, topic string, out chan<- *sarama.ConsumerMessage) error {
	partitions, err := consumer.SingleConsumer.Partitions(topic)
	if err != nil {
		log.Error().Err(err).Msg("failed to get partitions")
		return ErrGetPartitions
	}

	initialOffset := sarama.OffsetOldest
	if consumer.Config.OffsetNewest {
		initialOffset = sarama.OffsetNewest
	}

	for _, partition := range partitions {
		pc, err := consumer.SingleConsumer.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			log.Error().Err(err).Int32("partition", partition).Msg("error consuming partition")
			return ErrConsumePartition
		}

		go consume(ctx, pc, partition, out)
	}
	return nil
}

func consume(ctx context.Context, pc sarama.PartitionConsumer, partition int32, out chan<- *sarama.ConsumerMessage) {
	log.Info().Int32("partition", partition).Msg("consumer started")
	for {
		select {
		case <-ctx.Done():
			if err := pc.Close(); err != nil {
				log.Error().Err(err).Int32("partition", partition).Msg("failed to close consumer")
				return
			}
			log.Info().Int32("partition", partition).Msg("consumer closed")
			return
		case message := <-pc.Messages():
			out <- message
			log.Info().
				Str("topic", message.Topic).
				Int32("partition", message.Partition).
				Str("key", string(message.Key)).
				Int64("offset", message.Offset).
				Msg("kafka message claimed")
		}
	}
}
