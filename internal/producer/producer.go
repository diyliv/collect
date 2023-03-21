package producer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"

	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/pkg/utils"
)

type producer struct {
	logger   *zap.Logger
	producer *kafka.Writer
	cfg      *config.Config
}

func NewProducer(logger *zap.Logger, cfg *config.Config, topic string) *producer {
	return &producer{logger: logger, producer: &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: writerRequiredAcks,
		MaxAttempts:  writerMaxAttempts,
		Logger:       kafka.LoggerFunc(logger.Sugar().Debugf),
		ErrorLogger:  kafka.LoggerFunc(logger.Sugar().Errorf),
		Compression:  compress.Snappy,
		ReadTimeout:  writerReadTimeout,
		WriteTimeout: writerWriteTimeout,
	}, cfg: cfg}
}

func (p *producer) GetKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(p.cfg.Kafka.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: writerRequiredAcks,
		MaxAttempts:  writerMaxAttempts,
		Logger:       kafka.LoggerFunc(p.logger.Sugar().Debugf),
		ErrorLogger:  kafka.LoggerFunc(p.logger.Sugar().Errorf),
		Compression:  compress.Snappy,
		ReadTimeout:  writerReadTimeout,
		WriteTimeout: writerWriteTimeout,
	}
}

func (p *producer) Produce(ctx context.Context, message interface{}) error {
	switch t := message.(type) {
	case string:
		err := p.producer.WriteMessages(ctx, kafka.Message{Value: []byte(t)})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case time.Time:
		binTime, err := t.MarshalBinary()
		if err != nil {
			p.logger.Error("Error while converting time to binary: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: binTime})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case int32:
		err := p.producer.WriteMessages(ctx, kafka.Message{Value: []byte(strconv.Itoa(int(t)))})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case float64:
		err := p.producer.WriteMessages(ctx, kafka.Message{Value: utils.Float64ToBytes(t)})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	default:
		p.logger.Info(fmt.Sprintf("Undefined type: %T\n", t))
	}
	return nil
}
