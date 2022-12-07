package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
)

type delegate struct{}

func (d *delegate) NodeMeta(limit int) []byte {
	//TODO implement me
	panic("implement me")
}

func (d *delegate) NotifyMsg(bytes []byte) {
	//TODO implement me
	panic("implement me")
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	//TODO implement me
	panic("implement me")
}

func (d *delegate) LocalState(join bool) []byte {
	//TODO implement me
	panic("implement me")
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	//TODO implement me
	panic("implement me")
}

func (d *delegate) NotifyJoin(node *memberlist.Node) {
	fmt.Println("A node has joined: " + node.String())
}

func (d *delegate) NotifyLeave(node *memberlist.Node) {
	fmt.Println("A node has left: " + node.String())
}

func (d *delegate) NotifyUpdate(node *memberlist.Node) {
	fmt.Println("A node was updated: " + node.String())
}
