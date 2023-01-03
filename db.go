package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"github.com/samber/lo"
	"log"
	"os"
	"time"
)

type Index struct {
	Name     string   `json:"name"`
	Key      string   `json:"key"`
	JsonPath []string `json:"paths"`
}

func (idx Index) Validate() error {
	err := lo.Validate(len(idx.Name) > 0, "index name can not be emtpy")
	if err != nil {
		return err
	}
	err = lo.Validate(len(idx.Key) > 0, "bucket can not be emtpy")
	if err != nil {
		return err
	}
	err = lo.Validate(len(idx.JsonPath) > 0, "Json path can not be emtpy")
	if err != nil {
		return err
	}
	err = lo.Validate(!lo.ContainsBy(internal.ReversedIndex(), func(s string) bool {
		return s == idx.Name
	}), "can't use reserved index name %s", idx.Name)
	return err
}

type Options struct {
	// Local address to bind to
	Port int
	// Nodes all the members in the cluster
	Nodes []string
	// Replicas replication factor
	Replicas int
	Logger   *log.Logger
	Timeout  time.Duration
}

type DB interface {
	Set(k, v string) error
	SetWithTtl(k, v string, ttl time.Duration) error
	Get(k string) (string, time.Duration, error)
	Del(k string) error
	CreateJsonIndex(index Index) error
	DropIndex(name string) error
	Indexes() []Index
	Search(index string, exp string) (map[string]string, error)
	Shutdown()
}

func NewDB(options Options) (DB, error) {
	cfg := memberlist.DefaultLocalConfig()
	if options.Port > 0 {
		cfg.BindPort = options.Port
	} else {
		cfg.BindPort = DefaultPort
	}
	if options.Logger == nil {
		options.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	// default timeout is 4 * ProbeTimeout
	if options.Timeout == 0 {
		options.Timeout = cfg.ProbeTimeout * 4
	}
	cfg.Logger = options.Logger
	cfg.Name = fmt.Sprintf("%s-%d", cfg.Name, cfg.BindPort)
	storage, err := internal.NewStorage(options.Replicas, cfg.Logger)
	if err != nil {
		return nil, err
	}
	c := &cluster{
		storage: storage,
		options: options,
	}
	cfg.Events = &event{storage}
	cfg.Delegate = c
	members, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	// add db to db
	if len(options.Nodes) > 0 {
		members.Join(options.Nodes)
	}
	c.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return members.NumMembers()
		},
		RetransmitMult: 3,
	}
	c.members = members
	c.options = options
	return c, nil
}
