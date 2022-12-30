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

	//logger.Info("cluster join", zap.String("cluster", cluster.Address()),
	//	zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
	fmt.Println(fmt.Sprintf("cluster join %s", node.FullAddress()))
	//zap.L().Info("cluster join", zap.String("cluster", cluster.Address()))

}

func (evt *event) NotifyLeave(node *memberlist.Node) {
	evt.storage.Remove(node.Address())
	//logger.Info("cluster join", zap.String("cluster", cluster.Address()),
	//	zap.Int("num", len(evt.replication.GetMembers())), zap.Int("db nodes", evt.db.members.NumMembers()))
	//fmt.Print(fmt.Sprintf("cluster leave %s", cluster.FullAddress()))
	fmt.Println(fmt.Sprintf("cluster leave %s", node.FullAddress()))
	zap.L().Info("cluster leave", zap.String("cluster", node.Address()))
}

func (evt *event) NotifyUpdate(node *memberlist.Node) {
	////TODO implement me
	//panic("implement me")
	//fmt.Print(fmt.Sprintf("cluster update %s", cluster.FullAddress()))
	zap.L().Info("cluster update", zap.String("cluster", node.Address()))
}
