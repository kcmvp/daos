package daos

import (
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

type event struct {
	db          *DB
	replication *replication
}

func (evt *event) NotifyJoin(node *memberlist.Node) {
	evt.replication.Add(node)
	logger.Info("node join", zap.String("node", node.Address()),
		zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.replication.Remove(node.Address())
	logger.Info("node join", zap.String("node", node.Address()),
		zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	//TODO implement me
	panic("implement me")
}
