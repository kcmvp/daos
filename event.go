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

// @todo how to calculate the topology status
// @todo when in this status no read & write
// @todo how to calculate the data are portioned well

func (evt *event) NotifyJoin(node *memberlist.Node) {
	evt.storage.Add(node)
	evt.logger.Printf("[%s]:node join %s", evt.storage.Name(), node.Name)
	if evt.storage.Name() == node.Name {
		// @todo clean all the data
	} else {
		// @todo re-calculate the consistent and schedule to send to the owner
	}
}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.storage.Remove(node.Address())
	evt.logger.Printf("[%s]:node leave %s", evt.storage.Name(), node.Name)
	if evt.storage.Name() == node.Name {
		// @todo clean all the data
	} else {
		// @todo re-calculate the consistent and schedule to send to the owner
	}
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	////TODO implement me
	evt.logger.Printf("[%s]:node update %s", evt.storage.Name(), node.Name)
}
