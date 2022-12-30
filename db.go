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
	Bucket   string   `json:"key"`
	JsonPath []string `json:"paths"`
}

func (idx Index) Validate() error {
	err := lo.Validate(len(idx.Name) > 0, "index name can not be emtpy")
	if err != nil {
		return err
	}
	err = lo.Validate(len(idx.Bucket) > 0, "bucket can not be emtpy")
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
}

type DB interface {
	Set(k, v string) error
	SetWithTtl(k, v string, ttl time.Duration) error
	Get(k string) (string, error)
	Ttl(k string) (time.Duration, error)
	Del(k string)
	CreateJsonIndex(index Index) error
	DropIndex(name string) error
	Search(index string, by string) (map[string]string, error)
	Shutdown()
}

func NewDB(opt Options) (DB, error) {
	cfg := memberlist.DefaultLocalConfig()
	if opt.Port > 0 {
		cfg.BindPort = opt.Port
	} else {
		cfg.BindPort = DefaultPort
	}
	if opt.Logger == nil {
		opt.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	cfg.Logger = opt.Logger
	cfg.Name = fmt.Sprintf("%s-%d", cfg.Name, cfg.BindPort)
	storage, err := internal.NewStorage(opt.Replicas, cfg.Logger)
	if err != nil {
		return nil, err
	}
	c := &cluster{
		storage: storage,
		logger:  cfg.Logger,
	}
	cfg.Events = &event{storage}
	cfg.Delegate = c
	members, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	// add db to db
	if len(opt.Nodes) > 0 {
		members.Join(opt.Nodes)
	}
	c.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return members.NumMembers()
		},
		RetransmitMult: 3,
	}
	c.members = members
	return c, nil
}
