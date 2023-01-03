package daos

import (
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"log"
)

type event struct {
	storage *internal.Storage
	logger  *log.Logger
}

func (evt *event) NotifyJoin(node *memberlist.Node) {
	evt.storage.Add(node)
	evt.logger.Printf("node join %s", node.FullAddress())
}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.storage.Remove(node.Address())
	evt.logger.Printf("node leave %s", node.FullAddress())
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	////TODO implement me
	evt.logger.Printf("node update %s", node.FullAddress())
}
