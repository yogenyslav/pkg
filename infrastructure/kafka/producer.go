package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

// AsyncProducer is a Kafka async producer.
type AsyncProducer struct {
	Config   *Config
	producer sarama.AsyncProducer
}

// MustNewAsyncProducer creates a new Kafka async producer or panics if failed.
func MustNewAsyncProducer(config *Config, partitioner sarama.PartitionerConstructor, acks sarama.RequiredAcks) *AsyncProducer {
	cfg := sarama.NewConfig()

	cfg.Producer.Partitioner = partitioner
	cfg.Producer.RequiredAcks = acks

	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true

	brokers := make([]string, len(config.Brokers))
	for idx, broker := range config.Brokers {
		brokers[idx] = fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	}

	asyncProducer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		log.Panic().Err(err).Msg("kafka.NewAsyncProducer")
	}

	go func() {
		for e := range asyncProducer.Errors() {
			log.Error().Err(e).Msg("kafka.AsyncProducer")
		}
	}()

	return &AsyncProducer{
		Config:   config,
		producer: asyncProducer,
	}
}

// SendAsyncMessage sends a message to Kafka.
func (k *AsyncProducer) SendAsyncMessage(message *sarama.ProducerMessage) {
	k.producer.Input() <- message
}

// Close closes the Kafka async producer.
func (k *AsyncProducer) Close() {
	if err := k.producer.Close(); err != nil {
		log.Error().Err(err).Msg("kafka.AsyncProducer.Close")
	}
}
