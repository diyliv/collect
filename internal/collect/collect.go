package collect

import (
	"context"
	"fmt"
	"sync"

	"github.com/konimarti/opc"
	"go.uber.org/zap"

	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/internal/interfaces"
)

type collect struct {
	client opc.Connection
	logger *zap.Logger
	wg     sync.WaitGroup
	cfg    *config.Config
	write  interfaces.Producer
}

func NewCollect(client opc.Connection, logger *zap.Logger, cfg *config.Config, write interfaces.Producer) *collect {
	return &collect{
		client: client,
		logger: logger,
		wg:     sync.WaitGroup{},
		cfg:    cfg,
		write:  write,
	}
}

func (c *collect) ReadFromDA(ctx context.Context) {
	c.wg.Add(len(c.client.Tags()))
	for _, tag := range c.client.Tags() {
		go func(tag string) {
			defer c.wg.Done()
			item := c.client.ReadItem(tag)
			fmt.Println(tag, item.Value)
			if err := c.write.Produce(ctx, item.Value); err != nil {
				c.logger.Error("Error while producing message: " + err.Error())
			}
		}(tag)
	}
	c.wg.Wait()
}
