package daos

import (
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/memberlist"
	"net"
	"sync"
)

type NodeOptions struct {
	// Local address to bind to
	Port int
	// Members in the members
	Members []string
}

type Node struct {
	mtx        sync.RWMutex
	members    *memberlist.Memberlist
	caches     map[string]*Cache
	broadcasts *memberlist.TransmitLimitedQueue
}

func NewNode(opt NodeOptions) (*Node, error) {
	cache := new(Node)
	cfg := memberlist.DefaultLocalConfig()
	uuid, _ := uuid.GenerateUUID()
	ifaces, err := net.Interfaces()
	var ip net.IP
	for _, i := range ifaces {
		if addrs, err := i.Addrs(); err == nil {
			// handle err
			for _, addr := range addrs {
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
			}
		}
	}
	cfg.Name = ip.String() + "-" + uuid
	cfg.BindPort = opt.Port

	event := &delegate{}
	cfg.Events = event
	cfg.Delegate = event

	members, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}

	// add members to members
	if len(opt.Members) > 0 {
		members.Join(opt.Members)
	}

	br := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return members.NumMembers()
		},
		RetransmitMult: 3,
	}

	cache.members = members
	cache.broadcasts = br
	return cache, nil
}

func (node *Node) Join(members ...string) {

}

func (node *Node) Create(schema Schema) *Cache {
	c, ok := node.caches[schema.Name]
	if !ok {
		c = build(schema)
		node.caches[schema.Name] = c
	}
	return c
}

func (node *Node) Get(name string) *Cache {
	return nil
}
