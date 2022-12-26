package daos

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultPort = 7080
const DefaultUpdateTimeout = 50 * time.Millisecond
const NotFound = "_NF_"

type action int

const (
	set action = iota
	del
	ipcGet
	ipcSearch
	ipcRes
)

type Command struct {
	Action action        `json:"a"`
	Key    string        `json:"k"`
	Value  string        `json:"v"`
	TTL    time.Duration `json:"t"`
	Seq    uint64        `json:"s"`
	Caller string        `json:"c"`
	Index  string        `json:"i"`
}
type Options struct {
	// Local address to bind to
	Port int
	// Nodes all the members in the cluster
	Nodes    []string
	Replicas int
}

type DB struct {
	storage    *internal.Storage
	members    *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
}

func (db *DB) Ttl(k string) (time.Duration, error) {
	return db.storage.Ttl(k)
}

func (db *DB) NodeMeta(limit int) []byte {
	indexes, size := db.storage.Indexes()
	idxes := lo.DropRightWhile(indexes, func(item internal.Index) bool {
		if size > limit {
			t, _ := json.Marshal(item)
			size -= len(t)
			return true
		} else {
			return false
		}
	})
	data, _ := json.Marshal(idxes)
	return data
}

func (db *DB) NotifyMsg(bytes []byte) {
	c := Command{}
	if err := json.Unmarshal(bytes, &c); err != nil {
		zap.L().Error("incorrect user msg", zap.String("node", db.members.LocalNode().Address()),
			zap.String("msg", err.Error()))
	}
	switch c.Action {
	case set:
		if db.Primary(c.Key) {
			db.SetWithTtl(c.Key, c.Value, c.TTL)
		} else if db.Replicas(c.Key) {
			db.storage.Set(c.Key, c.Value, c.TTL)
		}
	case del:
		if db.Primary(c.Key) {
			db.Del(c.Key)
		} else if db.Replicas(c.Key) {
			db.storage.Del(c.Key)
		}
	case ipcGet:
		if db.Replicas(c.Key) {
			if v, err := db.storage.Get(c.Key); err == nil {
				caller, err := db.node(c.Caller)
				if err != nil {
					zap.L().Error("can not find the node", zap.String("from", db.members.LocalNode().Name),
						zap.String("to", c.Caller))
					return
				}
				ttl, _ := db.Ttl(c.Key)
				res := Command{
					Action: ipcRes,
					Key:    db.storage.IpcPrefix(c.Seq),
					Value:  v,
					TTL:    ttl,
				}
				data, _ := json.Marshal(res)
				db.members.SendReliable(caller, data)
			}
		}
	case ipcSearch:
		m := db.storage.SearchIndex(c.Index, c.Value)
		if len(m) == 0 {
			m[strconv.FormatUint(c.Seq, 10)] = NotFound
		}
		commands := lo.MapToSlice[string, string, Command](m, func(k, v string) Command {
			ttl, _ := db.Ttl(k)
			return Command{
				Action: ipcRes,
				Key:    fmt.Sprintf("%s:%s:%s", db.storage.IpcPrefix(c.Seq), db.members.LocalNode().Name, k),
				Value:  v,
				TTL:    ttl,
			}
		})
		data, _ := json.Marshal(commands)
		caller, _ := db.node(c.Caller)
		db.members.SendReliable(caller, data)
	case ipcRes:
		res := Command{}
		if err := json.Unmarshal(bytes, &res); err == nil {
			// keep the response 2 time.Millisecond
			db.storage.Set(res.Key, res.Value, 5*time.Millisecond)
		} else {
			zap.L().Error("failed to unmarshal the ipc_res", zap.String("data", string(bytes)))
		}
	}
}

func (db *DB) node(name string) (*memberlist.Node, error) {
	n, ok := lo.Find(db.members.Members(), func(n *memberlist.Node) bool {
		return n.Name == name
	})
	if ok {
		return n, nil
	} else {
		return nil, fmt.Errorf("can not find the node %s", name)
	}
}

func (db *DB) GetBroadcasts(overhead, limit int) [][]byte {
	return db.broadcasts.GetBroadcasts(overhead, limit)
}

func (db *DB) LocalState(join bool) []byte {
	indexes, _ := db.storage.Indexes()
	data, _ := json.Marshal(indexes)
	return data
}

func (db *DB) MergeRemoteState(buf []byte, join bool) {
	db.storage.MergeRemoteState(buf, join)
}

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (bc *broadcast) Invalidates(b memberlist.Broadcast) bool {
	return false
}

func (bc *broadcast) Message() []byte {
	return bc.msg
}

func (bc *broadcast) Finished() {
	if bc.notify != nil {
		close(bc.notify)
	}
}

func init() {
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)
	logger, err := config.Build()
	if err != nil {
		logger.Warn("failed to init logger", zap.String("msg", err.Error()))
	}
	defer logger.Sync()
	undo := zap.ReplaceGlobals(logger)
	defer undo()
}

func (db *DB) Set(k, v string) error {
	return db.SetWithTtl(k, v, 0*time.Second)
}

// SetWithTtl always set primary node first and then write to the replicas directly.
func (db *DB) SetWithTtl(k, v string, ttl time.Duration) error {
	var err error
	cmd := Command{Action: set, Key: k, Value: v, TTL: ttl}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// make sure write to primary first
	if db.Primary(k) {
		err = db.storage.Set(k, v, ttl)
		if err != nil {
			return err
		}
		replicas, _ := db.storage.Replicas(k)
		lop.ForEach(replicas, func(node *memberlist.Node, _ int) {
			if node.Name != db.members.LocalNode().Name {
				db.members.SendReliable(node, data)
			}
		})
	} else {
		primary, _ := db.storage.Primary(k)
		db.members.SendReliable(primary, data)
		zap.L().Info("redirect to primary node", zap.String("from", db.members.LocalNode().Name), zap.String("to", primary.Name))
	}
	return nil
}

func (db *DB) Replicas(key string) bool {
	nodes, _ := db.storage.Replicas(key)
	_, ok := lo.Find(nodes, func(item *memberlist.Node) bool {
		return item.Name == db.members.LocalNode().Name
	})
	return ok
}

func (db *DB) Primary(key string) bool {
	n, _ := db.storage.Primary(key)
	return n.Name == db.members.LocalNode().Name
}

func (db *DB) Get(k string) (string, error) {
	v, err := db.storage.Get(k)
	if err == nil {
		return v, nil
	} else {
		cmd := Command{Action: ipcGet, Key: k, Seq: rand.Uint64(), Caller: db.members.LocalNode().Name}
		data, _ := json.Marshal(cmd)
		primary, _ := db.storage.Primary(k)
		db.members.SendReliable(primary, data)
		return func(c Command) (string, error) {
			var v1 string
			var err1 error
			tries, duration, _ := lo.AttemptWithDelay(5, 200*time.Microsecond, func(_ int, d time.Duration) error {
				v1, err1 = db.storage.Get(db.storage.IpcPrefix(c.Seq))
				return err
			})
			if err != nil {
				zap.L().Error("[redirect]: ipc_get", zap.String("node", db.members.LocalNode().Address()),
					zap.String("remote", primary.Address()))
			} else {
				zap.L().Info("[redirect]: ipc_get", zap.String("node", db.members.LocalNode().Address()),
					zap.String("remote", primary.Address()), zap.Int("tries", tries),
					zap.Int64("duration(Microsecond)", int64(duration)/int64(time.Microsecond)))
			}
			// @todo update local node when applicable
			return v1, err1
		}(cmd)
	}
}

func (db *DB) Search(index, criteria string) (map[string]string, error) {
	// send ipc_search request to the cluster
	cmd := Command{
		Action: ipcSearch,
		Index:  index,
		Value:  criteria,
		Caller: db.members.LocalNode().Name,
		Seq:    rand.Uint64()}
	data, _ := json.Marshal(&cmd)
	lop.ForEach(db.members.Members(), func(r *memberlist.Node, _ int) {
		if r.Name != db.members.LocalNode().Name {
			db.members.SendReliable(r, data)
		}
	})
	// ipc_get remote result
	return func(c Command) (map[string]string, error) {
		lr := db.storage.SearchIndex(c.Index, c.Value)
		rr := map[string]string{}
		_, _, err := lo.AttemptWithDelay(6, 300*time.Microsecond, func(_ int, d time.Duration) error {
			rt := db.storage.ScanIPC(c.Seq)
			nm := map[string]int{}
			for _k, v := range rt {
				ks := strings.Split(_k, ":")
				n := ks[len(ks)-2]
				nm[n] = 1
				k := ks[len(ks)-1]
				if strconv.FormatUint(c.Seq, 10) != k && v != NotFound {
					p, _ := db.storage.Primary(k)
					_, ok := rr[k]
					if p.Name == n || !ok {
						rr[k] = v
					}
				}
			}
			if len(nm)+1 == len(db.members.Members()) {
				return nil
			}
			return fmt.Errorf("time out")
		})
		if err != nil {
			rr = map[string]string{}
		} else {
			for k, v := range lr {
				if _v, ok := rr[k]; !ok {
					rr[k] = v
				} else if v != _v {
					n, _ := db.storage.Primary(k)
					if n.Name == db.members.LocalNode().Name {
						rr[k] = v
					} else {
						// update the latest
						// @todo update local node when applicable
						// @todo need to ipc_get ttl of the key
					}
				}
			}
		}
		return rr, err
	}(cmd)
}

func (db *DB) Del(k string) {
	cmd := Command{Action: del, Key: k}
	data, _ := json.Marshal(cmd)
	if db.Primary(k) {
		db.storage.Del(k)
		replicas, _ := db.storage.Replicas(k)
		lop.ForEach(replicas, func(node *memberlist.Node, _ int) {
			if node.Name != db.members.LocalNode().Name {
				db.members.SendReliable(node, data)
			}
		})
	} else {
		primary, _ := db.storage.Primary(k)
		db.members.SendReliable(primary, data)
	}
}

func (db *DB) CreateJsonIndex(name, pattern string, paths ...string) error {
	now := time.Now()
	index := internal.Index{
		Name:    name,
		Key:     pattern,
		Paths:   paths,
		Version: now.Unix(),
	}
	err := db.storage.CreateIndex(index)
	if err != nil {
		return err
	}
	err = db.members.UpdateNode(DefaultUpdateTimeout)
	if err != nil {
		zap.L().Error("failed to broadcast index creation", zap.String("node", db.members.LocalNode().Address()))
	}
	return err
}

func (db *DB) DropIndex(name string) error {
	err := db.storage.DropIndex(name)
	if err != nil {
		return err
	}
	err = db.members.UpdateNode(DefaultUpdateTimeout)
	if err != nil {
		zap.L().Error("failed to broadcast index deletion", zap.String("node", db.members.LocalNode().Address()),
			zap.String("index", name))
	}
	return err
}

func NewDB(opt Options) (*DB, error) {

	hostname, _ := os.Hostname()
	cfg := memberlist.DefaultLocalConfig()
	if opt.Port > 0 {
		cfg.BindPort = opt.Port
	} else {
		cfg.BindPort = DefaultPort
	}

	cfg.Name = fmt.Sprintf("%storage-%d", hostname, cfg.BindPort)

	storage, err := internal.NewStorage(opt.Replicas)
	if err != nil {
		return nil, err
	}

	db := new(DB)
	db.storage = storage

	cfg.Events = &event{storage}
	cfg.Delegate = db
	members, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}
	// add node to cluster
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

var _ Cache = (*DB)(nil)
var _ memberlist.Broadcast = (*broadcast)(nil)
