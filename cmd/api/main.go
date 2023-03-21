package main

import (
	"context"
	"log"

	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/internal/collect"
	"github.com/diyliv/collect/internal/producer"
	"github.com/diyliv/collect/pkg/kafka"
	"github.com/diyliv/collect/pkg/logger"
	"github.com/diyliv/collect/pkg/opcda"
)

func main() {
	ctx := context.Background()
	cfg := config.ReadConfig("config", "yaml", "./config")
	client, err := opcda.ConnectOPCDA(cfg.OPCDA.ProgId, cfg.OPCDA.Nodes, cfg.OPCDA.Tags)
	if err != nil {
		log.Printf("Error while connecting to OPC-DA server: %v\n", err)
		return
	}
	logger := logger.InitLogger()
	kafka, err := kafka.NewKafkaConn(cfg)
	if err != nil {
		log.Printf("Error while creating new kafka conn: %v\n", err)
		return
	}
	kafkaProducer := producer.NewProducer(logger, cfg, "topic")
	defer func() {
		client.Close()
		kafka.Close()
	}()
	collect := collect.NewCollect(client, logger, cfg, kafkaProducer)
	collect.ReadFromDA(ctx)
}
