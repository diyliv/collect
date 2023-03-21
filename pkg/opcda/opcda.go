package opcda

import (
	"github.com/konimarti/opc"
)

func ConnectOPCDA(progId string, node, tags []string) (opc.Connection, error) {
	opc.Debug()
	client, err := opc.NewConnection(progId, node, tags)
	if err != nil {
		return nil, err
	}
	return client, err
}
