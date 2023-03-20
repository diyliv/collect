package main

import (
	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/internal/collect"
	"github.com/diyliv/collect/internal/producer"
	"github.com/diyliv/collect/pkg/kafka"
	"github.com/diyliv/collect/pkg/logger"
)

func main() {
	cfg := config.ReadConfig("config", "yaml", "./config")
	logger := logger.InitLogger()
	kafka, err := kafka.NewKafkaProducer(cfg)
	if err != nil {
		panic(err)
	}
	kafkaProducer := producer.NewProducer(logger, kafka)
	collect := collect.NewCollect(cfg, logger, kafkaProducer)
	collect.ReadFromDA()
}
