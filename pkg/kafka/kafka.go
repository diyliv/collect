package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/diyliv/collect/config"
)

func NewKafkaProducer(cfg *config.Config) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.Brokers[0],
	})
	if err != nil {
		return nil, err
	}
	return producer, err
}
