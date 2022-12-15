package daos

import (
	"encoding/json"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
	"go.uber.org/zap"
	"net"
	"strconv"
	"sync"
	"time"
)

const DefaultPort = 7080
const defaultPartitionCount = 1357
const defaultUpdateTimeout = 50 * time.Millisecond

const indexMeta = "_idx_meta_"
const indexMetaPrefix = "_idx_:"
const metaVersion = "version"

var logger, _ = zap.NewProduction()

type Options struct {
	// Local address to bind to
	Port int
	// Nodes all the members in the cluster
	Nodes    []string
	Replicas int
}

type DB struct {
	storage    *buntdb.DB
	mtx        sync.RWMutex
	members    *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	router     *consistent.Consistent
	replicas   int
}

func (db *DB) replicasNode(key string) ([]*memberlist.Node, error) {
	var nodes []*memberlist.Node
	retry.Do(func() error {
		if len(db.members.Members()) == len(db.router.GetMembers()) {
			members, err := db.router.GetClosestN([]byte(key), db.replicas)
			if err != nil {
				return err
			}
			nodes = lo.Map[consistent.Member, *memberlist.Node](members, func(m consistent.Member, index int) *memberlist.Node {
				v, _ := m.(*memberlist.Node)
				return v
			})
			return nil
		} else {
			logger.Error("cluster is not in stable status",
				zap.String("node", db.members.LocalNode().Address()),
				zap.Int("members.Members", len(db.members.Members())),
				zap.Int("consistent.members", len(db.router.GetMembers())))
			return fmt.Errorf("cluster is not in stable status %d, %d", len(db.members.Members()), len(db.router.GetMembers()))
		}
	}, retry.Attempts(2), retry.Delay(5*time.Millisecond))

	return nodes, nil
}

func (db *DB) Set(k, v string, ttl time.Duration) {
	m := db.router.LocateKey([]byte(k))
	if m.String() == "" {

	} else {
		// redirect to the primary
	}
}

func (db *DB) Get(k string) string {
	var value string
	var err error
	err = db.storage.View(func(tx *buntdb.Tx) error {
		value, err = tx.Get(k)
		return err
	})
	if err == nil {
		return value
	} else {
		//  todo return from replicas
	}

	panic("implement me")
}

func (db *DB) Del(k string) (string, error) {
	var v string
	var err error
	err = db.storage.Update(func(tx *buntdb.Tx) error {
		v, err = tx.Delete(k)
		return err
	})
	if err != nil {
		logger.Error("failed to delete key", zap.String("node", db.members.LocalNode().Address()), zap.String("key", k))
		return v, err
	}
	members, err := db.router.GetClosestN([]byte(k), db.replicas)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to get replicas %s", err.Error()), zap.String("node", db.members.LocalNode().Address()))
		return "", err
	}
	go func() {
		msg := action{C: del, K: k}
		d, _ := json.Marshal(msg)
		for _, m := range members {
			n, _ := m.(*memberlist.Node)
			db.members.SendBestEffort(n, d)
		}
	}()
	return v, err

}

func (db *DB) CreateJsonIndex(name, pattern string, paths ...string) error {
	names, err := db.storage.Indexes()
	if err != nil {
		return err
	}
	_, ok := lo.Find[string](names, func(n string) bool {
		return n == name
	})
	if ok {
		logger.Warn("index exists", zap.String("node", db.members.LocalNode().Address()),
			zap.String("index", name))
		return fmt.Errorf("index %s exists", name)
	}
	idxes := lo.Map[string, func(a, b string) bool](paths, func(p string, _ int) func(a, b string) bool {
		return buntdb.IndexJSON(p)
	})
	db.storage.CreateIndex(name, pattern, idxes...)
	idx := &index{name, pattern, []string{}, time.Now().Unix()}
	lo.ForEach(paths, func(p string, _ int) {
		idx.Paths = append(idx.Paths, p)
	})

	data, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	db.storage.CreateIndex(indexMeta, fmt.Sprintf("%s*", indexMetaPrefix), buntdb.IndexJSON(metaVersion))
	err = db.storage.Update(func(tx *buntdb.Tx) error {
		tx.Set(fmt.Sprintf("%s%s", indexMetaPrefix, name), string(data), nil)
		return nil
	})
	if err != nil {
		return err
	}
	err = db.members.UpdateNode(defaultUpdateTimeout)
	if err != nil {
		logger.Error("failed to broadcast index message", zap.String("node", db.members.LocalNode().Address()))
	}
	return err
}

func (db *DB) DropIndex(name string) error {
	err := db.storage.DropIndex(name)
	if err != nil {
		return err
	}
	err = db.storage.Update(func(tx *buntdb.Tx) error {
		_, err = tx.Delete(fmt.Sprintf("%s%s", indexMetaPrefix, name))
		return err
	})
	if err != nil {
		return err
	}
	err = db.members.UpdateNode(defaultUpdateTimeout)
	if err != nil {
		logger.Error("failed to delete the index", zap.String("node", db.members.LocalNode().Address()),
			zap.String("index", name))
	}
	return err
}

func (db *DB) SearchIndexBy(index string, value string) map[string]string {
	//TODO implement me
	db.storage.View(func(tx *buntdb.Tx) error {
		return tx.AscendEqual(index, value, func(key, value string) bool {
			return true
		})
	})
	panic("board cast")
}

func NewDB(opt Options) (*DB, error) {
	db := new(DB)
	ifs, err := net.Interfaces()
	if err != nil {
		logger.Error("failed to get ip", zap.String("db", "_"), zap.String("cause", err.Error()))
		return nil, err
	}
	var ip net.IP
	for _, i := range ifs {
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
	storage, err := buntdb.Open(":memory:")
	if err != nil {
		logger.Error("failed to create database", zap.String("db", ip.String()), zap.String("cause", err.Error()))
		return nil, err
	}
	db.storage = storage
	db.replicas = opt.Replicas
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindPort = func() int {
		if opt.Port > 0 {
			return opt.Port
		} else {
			return DefaultPort
		}
	}()
	cfg.Name = net.JoinHostPort(ip.String(), strconv.Itoa(cfg.BindPort))

	cfg.Events = &event{db}
	cfg.Delegate = &delegate{db}

	members, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}

	// add db to members
	if len(opt.Nodes) > 0 {
		members.Join(opt.Nodes)
	}

	db.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return members.NumMembers()
		},
		RetransmitMult: 3,
	}
	db.members = members
	db.router = consistent.New(nil, consistent.Config{
		PartitionCount: defaultPartitionCount,
		Hasher:         &hash{},
	})
	return db, nil
}

type hash struct{}

func (h hash) Sum64(bytes []byte) uint64 {
	return xxhash.Sum64(bytes)
}

// interface guard
var _ consistent.Hasher = (*hash)(nil)
var _ Cache = (*DB)(nil)
