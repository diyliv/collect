package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"

	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/internal/models"
)

type producer struct {
	logger   *zap.Logger
	producer *kafka.Writer
	cfg      *config.Config
}

func NewProducer(logger *zap.Logger, cfg *config.Config, topic string) *producer {
	return &producer{logger: logger,
		producer: &kafka.Writer{
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
		},
		cfg: cfg}
}

func (p *producer) Produce(ctx context.Context, serverName, tagName string, itemQuality int16, readAt time.Time, message interface{}) error {
	switch t := message.(type) {
	case string:
		opcByte, err := marshal(models.OPCDA{
			ServerName: serverName,
			TagName:    tagName,
			TagType:    reflect.TypeOf(t).String(),
			TagValue:   t,
			TagQuality: itemQuality,
			ReadAt:     readAt,
		})
		if err != nil {
			p.logger.Error("Error while marshalling: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: opcByte})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case time.Time:
		opcByte, err := marshal(models.OPCDA{
			ServerName: serverName,
			TagName:    tagName,
			TagType:    reflect.TypeOf(t).String(),
			TagValue:   t,
			TagQuality: itemQuality,
			ReadAt:     readAt,
		})
		if err != nil {
			p.logger.Error("Error while marshalling: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: opcByte})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case int32:
		opcByte, err := marshal(models.OPCDA{
			ServerName: serverName,
			TagName:    tagName,
			TagType:    reflect.TypeOf(t).String(),
			TagValue:   t,
			TagQuality: itemQuality,
			ReadAt:     readAt,
		})
		if err != nil {
			p.logger.Error("Error while marshalling: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: opcByte})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case float64:
		opcByte, err := marshal(models.OPCDA{
			ServerName: serverName,
			TagName:    tagName,
			TagType:    reflect.TypeOf(t).String(),
			TagValue:   t,
			TagQuality: itemQuality,
			ReadAt:     readAt,
		})
		if err != nil {
			p.logger.Error("Error while marshalling: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: opcByte})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	case float32:
		opcByte, err := marshal(models.OPCDA{
			ServerName: serverName,
			TagName:    tagName,
			TagType:    reflect.TypeOf(t).String(),
			TagValue:   t,
			TagQuality: itemQuality,
			ReadAt:     readAt,
		})
		if err != nil {
			p.logger.Error("Error while marshalling: " + err.Error())
			return err
		}
		err = p.producer.WriteMessages(ctx, kafka.Message{Value: opcByte})
		if err != nil {
			p.logger.Error("Error while writing messages: " + err.Error())
			return err
		}
	default:
		p.logger.Info(fmt.Sprintf("Undefined type: %T\n", t))
	}
	return nil
}

func marshal(data interface{}) ([]byte, error) {
	dataByte, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return dataByte, err
}
