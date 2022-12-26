package internal

import (
	"encoding/json"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
	"go.uber.org/zap"
	"strings"
	"time"
)

const defaultPartitionCount = 1001
const indexIndexName = "_idx_meta_"
const indexPrefix = "_idx_:"
const indexVersion = "version"

const ipcIndexName = "_idx_ipc"
const ipcKeyPrefix = "_ipc_:"

type Storage struct {
	db       *buntdb.DB
	replicas int
	*consistent.Consistent
}

type Index struct {
	Name    string   `json:"name"`
	Key     string   `json:"key"`
	Paths   []string `json:"paths"`
	Version int64    `json:"version"`
}

func (s *Storage) Set(k, v string, ttl time.Duration) error {
	var err error
	s.db.Update(func(tx *buntdb.Tx) error {
		if ttl > 0 {
			_, _, err = tx.Set(k, v, &buntdb.SetOptions{true, ttl})
		} else {
			_, _, err = tx.Set(k, v, nil)
		}
		return err
	})
	return err
}

func (s *Storage) Get(k string) (v string, err error) {
	s.db.View(func(tx *buntdb.Tx) error {
		v, err = tx.Get(k, false)
		return err
	})
	return v, err
}

func (s *Storage) Ttl(k string) (time.Duration, error) {
	var ttl time.Duration
	var err error
	s.db.View(func(tx *buntdb.Tx) error {
		ttl, err = tx.TTL(k)
		return err
	})
	return ttl, err
}

func (s *Storage) IpcPrefix(seq uint64) string {
	return fmt.Sprintf("%s%d", ipcKeyPrefix, seq)
}

func (s *Storage) SearchIndex(index, criteria string) map[string]string {
	rsm := map[string]string{}
	s.db.View(func(tx *buntdb.Tx) error {
		tx.AscendEqual(index, criteria, func(key, value string) bool {
			rsm[key] = value
			return true
		})
		return nil
	})
	return rsm
}

func (s *Storage) ScanIPC(seq uint64) map[string]string {
	return s.ScanIndex(ipcIndexName, s.IpcPrefix(seq))
}

func (s *Storage) ScanIndex(index, prefix string) map[string]string {
	rsm := map[string]string{}
	s.db.View(func(tx *buntdb.Tx) error {
		tx.Ascend(index, func(key, value string) bool {
			if strings.HasPrefix(key, prefix) {
				rsm[key] = value
			}
			return true
		})
		return nil
	})
	return rsm
}

func (s *Storage) Del(k string) (v string, err error) {
	s.db.Update(func(tx *buntdb.Tx) error {
		v, err = tx.Delete(k)
		return nil
	})
	return
}

func (s *Storage) Indexes() ([]Index, int) {
	var indexes []Index
	var size int
	s.db.View(func(tx *buntdb.Tx) error {
		tx.Ascend(indexIndexName, func(k, v string) bool {
			index := Index{}
			size += len(v)
			json.Unmarshal([]byte(v), &index)
			indexes = append(indexes, index)
			return true
		})
		return nil
	})
	return indexes, size
}

func (s *Storage) CreateIndex(index Index) error {
	var err error
	s.DropIndex(index.Name)
	err = s.db.Update(func(tx *buntdb.Tx) error {
		var data []byte
		jsonIndex := lo.Map[string, func(a, b string) bool](index.Paths, func(p string, _ int) func(a, b string) bool {
			return buntdb.IndexJSON(p)
		})
		err = tx.CreateIndex(index.Name, index.Key, jsonIndex...)
		data, err = json.Marshal(index)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(fmt.Sprintf("%s%s", indexPrefix, index.Name), string(data), nil)
		return err
	})
	return err
}

func (s *Storage) DropIndex(name string) error {
	var err error
	err = s.db.Update(func(tx *buntdb.Tx) error {
		err = tx.DropIndex(name)
		if err != nil {
			return err
		}
		_, err = tx.Delete(fmt.Sprint("%s%s", indexPrefix, name))
		return err
	})
	return err
}

func (s *Storage) MergeRemoteState(buf []byte, join bool) {
	var indexes []Index
	var existing []Index
	json.Unmarshal(buf, &indexes)
	lo.ForEach(indexes, func(item Index, _ int) {
		v, err := s.Get(fmt.Sprintf("%s%s", indexPrefix, item.Name))
		var idx Index
		if err == nil {
			json.Unmarshal([]byte(v), &idx)
			existing = append(existing, idx)
		}
	})
	var update []Index
	lo.ForEach(indexes, func(r Index, _ int) {
		f := lo.ContainsBy(existing, func(e Index) bool {
			return r.Name == e.Name && r.Version <= e.Version
		})
		if !f {
			update = append(update, r)
		}
	})
	lo.ForEach(update, func(i Index, _ int) {
		s.CreateIndex(i)
	})

}
func NewStorage(replicas int) (*Storage, error) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		zap.L().Error("failed to create s", zap.String("cause", err.Error()))
		return nil, err
	}
	err = db.Update(func(tx *buntdb.Tx) error {
		tx.CreateIndex(ipcIndexName, fmt.Sprintf("%s*", ipcKeyPrefix), buntdb.IndexString)
		return tx.CreateIndex(indexIndexName, fmt.Sprintf("%s*", indexPrefix), buntdb.IndexJSON(indexVersion))
	})
	if err != nil {
		panic("failed to init database")
	}
	s := &Storage{
		db,
		replicas,
		consistent.New(nil, consistent.Config{
			PartitionCount: defaultPartitionCount,
			Hasher:         &hash{},
		}),
	}
	return s, nil
}

func (r *Storage) Primary(k string) (*memberlist.Node, error) {
	n, ok := r.LocateKey([]byte(k)).(*memberlist.Node)
	if ok {
		return n, nil
	} else {
		return nil, fmt.Errorf("failed to get the primay node of key")
	}
}

func (r *Storage) Replicas(k string) ([]*memberlist.Node, error) {
	var nodes []*memberlist.Node
	members, err := r.GetClosestN([]byte(k), r.replicas)
	if err != nil {
		zap.L().Error("failed to get replicated node of key", zap.String("key", k))
	}
	nodes = lo.Map[consistent.Member, *memberlist.Node](members, func(m consistent.Member, index int) *memberlist.Node {
		v, _ := m.(*memberlist.Node)
		return v
	})
	return nodes, err

}

type hash struct{}

func (h hash) Sum64(bytes []byte) uint64 {
	return xxhash.Sum64(bytes)
}
