package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack/v5"
)

type merge struct {
	meta []byte
}

func (m *merge) NotifyMerge(peers []*memberlist.Node) error {
	var local Options
	var remote Options
	msgpack.Unmarshal(m.meta, &local)
	msgpack.Unmarshal(peers[0].Meta, &remote)
	if local.Partitions == remote.Partitions &&
		local.Timeout == remote.Timeout &&
		local.Retry == remote.Retry &&
		local.Replicas == remote.Replicas {
		return nil
	} else {
		return fmt.Errorf("inconsistent configuration %s, %s", m.meta, peers[0].Meta)
	}
}

var _ memberlist.MergeDelegate = (*merge)(nil)
