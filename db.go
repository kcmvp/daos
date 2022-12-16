package daos

import (
	"encoding/json"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"github.com/tidwall/buntdb"
	"go.uber.org/zap"
	"net"
	"strconv"
	"sync"
	"time"
)

const DefaultPort = 7080
const defaultPartitionCount = 1001
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
	storage     *buntdb.DB
	mtx         sync.RWMutex
	members     *memberlist.Memberlist
	broadcasts  *memberlist.TransmitLimitedQueue
	replication *replication
}

func (db *DB) save(k, v string, ttl ...time.Duration) error {
	t := 0 * time.Second
	if len(ttl) > 0 {
		t = ttl[0]
	}
	db.storage.Update(func(tx *buntdb.Tx) error {
		if t > 0 {
			tx.Set(k, v, &buntdb.SetOptions{
				true,
				t,
			})
		} else {
			tx.Set(k, v, nil)
		}
		return nil
	})
	return nil
}

func (db *DB) Set(k, v string, ttl ...time.Duration) error {
	n, err := db.replication.primary(k)
	if db.members.LocalNode() == n {
		return db.save(k, v, ttl...)
	} else {
		cmd := command{set, k, v, ttl}
		data, _ := json.Marshal(cmd)
		nodes, _ := db.replication.replicas(k)
		lop.ForEach(nodes, func(n *memberlist.Node, _ int) {
			err = db.members.SendReliable(n, data)
		})
	}
	return err
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
		// @todo return from replication
		// @todo grpc
		panic("implement me")
	}
}

func (db *DB) delete(k string) (string, error) {
	var v string
	var err error
	err = db.storage.Update(func(tx *buntdb.Tx) error {
		v, err = tx.Delete(k)
		return err
	})
	return v, err
}

func (db *DB) Del(k string) (string, error) {
	var v string
	var err error
	n, _ := db.replication.LocateKey([]byte(k)).(*memberlist.Node)
	if db.members.LocalNode() == n {
		v, err = db.delete(k)
		if err != nil {
			logger.Error("failed to delete key", zap.String("node", db.members.LocalNode().Address()), zap.String("key", k))
		}
	}
	nodes, err := db.replication.replicas(k)
	cmd := command{A: del, K: k}
	d, _ := json.Marshal(cmd)
	lop.ForEach(nodes, func(m *memberlist.Node, _ int) {
		if err = db.members.SendReliable(n, d); err != nil {
			logger.Error("failed to send delete command", zap.String("node", db.members.LocalNode().Address()),
				zap.String("msg", err.Error()))
		}
	})
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
	cfg := memberlist.DefaultLocalConfig()
	cfg.BindPort = func() int {
		if opt.Port > 0 {
			return opt.Port
		} else {
			return DefaultPort
		}
	}()
	cfg.Name = net.JoinHostPort(ip.String(), strconv.Itoa(cfg.BindPort))

	replicas := &replication{
		opt.Replicas,
		consistent.New(nil, consistent.Config{
			PartitionCount: defaultPartitionCount,
			Hasher:         &hash{},
		}),
	}
	db.replication = replicas
	cfg.Events = &event{db, replicas}
	cfg.Delegate = &delegate{db, replicas}

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
	return db, nil
}

type hash struct{}

func (h hash) Sum64(bytes []byte) uint64 {
	return xxhash.Sum64(bytes)
}

// interface guard
var _ consistent.Hasher = (*hash)(nil)
var _ Cache = (*DB)(nil)
