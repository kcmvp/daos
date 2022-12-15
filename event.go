package daos

import (
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

type event struct {
	db *DB
}

func (evt *event) NotifyJoin(node *memberlist.Node) {
	logger.Info("node join", zap.String("node", node.Address()))
	evt.db.router.Add(node)
}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.db.router.Remove(node.Address())
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}
