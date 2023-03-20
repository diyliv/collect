package collect

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/diyliv/collect/config"
	"github.com/diyliv/collect/internal/interfaces"
	"github.com/diyliv/collect/pkg/opcda"
)

type collect struct {
	cfg    *config.Config
	logger *zap.Logger
	write  interfaces.Producer
}

func NewCollect(cfg *config.Config, logger *zap.Logger, write interfaces.Producer) *collect {
	return &collect{
		cfg:    cfg,
		logger: logger,
		write:  write,
	}
}

func (c *collect) ReadFromDA() {
	client, err := opcda.ConnectOPCDA(c.cfg.OPCDA.ProgId, c.cfg.OPCDA.Nodes, c.cfg.OPCDA.Tags)
	if err != nil {
		c.logger.Error(fmt.Sprintf("Error while connecting to: ProgId[%s] Node %s %v\n",
			c.cfg.OPCDA.ProgId, c.cfg.OPCDA.Nodes, err))
	}
	defer client.Close()

	for _, tag := range client.Tags() {
		if err := c.write.Produce([]string{tag}); err != nil {
			c.logger.Error("Error while producing message: " + err.Error())
		}
	}
}
