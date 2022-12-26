package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"go.uber.org/zap"
)

type event struct {
	storage *internal.Storage
}

func (evt *event) NotifyJoin(node *memberlist.Node) {
	evt.storage.Add(node)

	//logger.Info("node join", zap.String("node", node.Address()),
	//	zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
	fmt.Println(fmt.Sprintf("node join %s", node.FullAddress()))
	//zap.L().Info("node join", zap.String("node", node.Address()))

}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.storage.Remove(node.Address())
	//logger.Info("node join", zap.String("node", node.Address()),
	//	zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
	//fmt.Print(fmt.Sprintf("node leave %s", node.FullAddress()))
	fmt.Println(fmt.Sprintf("node leave %s", node.FullAddress()))
	zap.L().Info("node leave", zap.String("node", node.Address()))
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	////TODO implement me
	//panic("implement me")
	//fmt.Print(fmt.Sprintf("node update %s", node.FullAddress()))
	zap.L().Info("node update", zap.String("node", node.Address()))
}
