package daos

import (
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type replication struct {
	num int
	*consistent.Consistent
}

func (r *replication) primary(k string) (*memberlist.Node, error) {
	n, ok := r.LocateKey([]byte(k)).(*memberlist.Node)
	if ok {
		return n, nil
	} else {
		return nil, fmt.Errorf("failed to get the primay node of key")
	}
}

func (r *replication) replicas(k string) ([]*memberlist.Node, error) {
	var nodes []*memberlist.Node
	members, err := r.GetClosestN([]byte(k), r.num)
	if err != nil {
		logger.Error("failed to get replicated node of key", zap.String("key", k))
	}
	nodes = lo.Map[consistent.Member, *memberlist.Node](members, func(m consistent.Member, index int) *memberlist.Node {
		v, _ := m.(*memberlist.Node)
		return v
	})
	return nodes, err

}
