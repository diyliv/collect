package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type producer struct {
	logger   *zap.Logger
	producer *kafka.Producer
}

func NewProducer(logger *zap.Logger, kafkaProducer *kafka.Producer) *producer {
	return &producer{logger: logger, producer: kafkaProducer}
}

func (p *producer) Produce(messages []string) error {
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					p.logger.Error(fmt.Sprintf("Delivery failed: %v\n", ev.TopicPartition))
				} else {
					p.logger.Info(fmt.Sprintf("Delivered message to: %v\n", ev.TopicPartition))
				}
			}
		}
	}()

	topic := "topic"
	for _, msg := range messages {
		if err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil); err != nil {
			p.logger.Error("Error while producing message: " + err.Error())
			return err
		}
	}
	p.producer.Flush(15 * 1000)
	return nil
}
