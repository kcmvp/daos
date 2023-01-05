package internal

import (
	"encoding/json"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
	"log"
	"strings"
	"time"
)

const indexIndexName = "_meta_"
const indexKeyPrefix = "_idx_:"
const indexVersion = "version"

func ReversedIndex() []string {
	return []string{indexIndexName}
}

func ReversedBucket() []string {
	return []string{indexKeyPrefix}
}

type IdxMeta struct {
	Name     string   `json:"name"`
	Key      string   `json:"key"`
	JsonPath []string `json:"paths"`
	Version  int64    `json:"version"`
}

type Storage struct {
	name     string
	replicas int
	logger   *log.Logger
	*buntdb.DB
	*consistent.Consistent
}

type Row struct {
	Key   string        `json:"k,omitempty"`
	Value string        `json:"v,omitempty"`
	TTL   time.Duration `json:"t,omitempty"`
}

func (s *Storage) Name() string {
	return s.name
}

func (s *Storage) Set(k, v string, ttl time.Duration) error {
	var err error
	s.Update(func(tx *buntdb.Tx) error {
		if ttl > 0 {
			_, _, err = tx.Set(k, v, &buntdb.SetOptions{Expires: true, TTL: ttl})
		} else {
			_, _, err = tx.Set(k, v, nil)
		}
		return err
	})
	return err
}

func (s *Storage) Get(k string) (v string, ttl time.Duration, err error) {
	s.View(func(tx *buntdb.Tx) error {
		v, err = tx.Get(k, false)
		if err != nil {
			return err
		}
		ttl, err = tx.TTL(k)
		return err
	})
	if err != nil {
		v = ""
	}
	return
}

func (s *Storage) Ttl(k string) (time.Duration, error) {
	var ttl time.Duration
	var err error
	s.View(func(tx *buntdb.Tx) error {
		ttl, err = tx.TTL(k)
		return err
	})
	return ttl, err
}

func (s *Storage) SearchIndex(index, criteria string) []Row {
	var rows []Row
	s.View(func(tx *buntdb.Tx) error {
		tx.AscendEqual(index, criteria, func(key, value string) bool {
			// filter out replicas
			if primary, err := s.Primary(key); err == nil && primary.Name == s.name {
				ttl, _ := tx.TTL(key)
				r := Row{
					Key:   key,
					Value: value,
					TTL:   ttl,
				}
				rows = append(rows, r)
			}
			return true
		})
		return nil
	})
	return rows
}

func (s *Storage) ScanIndex(index, indexPrefix string) []Row {
	var resp []Row
	s.View(func(tx *buntdb.Tx) error {
		tx.Ascend(index, func(key, value string) bool {
			if strings.HasPrefix(key, indexPrefix) {
				ttl, _ := tx.TTL(key)
				r := Row{
					Key:   key,
					Value: value,
					TTL:   ttl,
				}
				resp = append(resp, r)
			}
			return true
		})
		return nil
	})
	return resp
}

func (s *Storage) Del(k string) (v string, err error) {
	s.Update(func(tx *buntdb.Tx) error {
		v, err = tx.Delete(k)
		return nil
	})
	return
}

func (s *Storage) Indexes() ([]IdxMeta, int) {
	var indexes []IdxMeta
	var size int
	s.View(func(tx *buntdb.Tx) error {
		tx.Ascend(indexIndexName, func(k, v string) bool {
			index := IdxMeta{}
			size += len(v)
			json.Unmarshal([]byte(v), &index)
			indexes = append(indexes, index)
			return true
		})
		return nil
	})
	return indexes, size
}
func (s *Storage) CreateIndex(index IdxMeta) error {
	var err error
	err = s.Update(func(tx *buntdb.Tx) error {
		//var data []byte
		jsonIndex := lo.Map[string, func(a, b string) bool](index.JsonPath, func(p string, _ int) func(a, b string) bool {
			return buntdb.IndexJSON(p)
		})
		err = tx.CreateIndex(index.Name, index.Key, jsonIndex...)
		if err != nil {
			return err
		}
		data, err := json.Marshal(index)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(fmt.Sprintf("%s%s", indexKeyPrefix, index.Name), string(data), nil)
		return err
	})
	return err
}

func (s *Storage) DropIndex(name string) error {
	var err error
	err = s.Update(func(tx *buntdb.Tx) error {
		err = tx.DropIndex(name)
		if err != nil {
			return err
		}
		_, err = tx.Delete(fmt.Sprintf("%s%s", indexKeyPrefix, name))
		return err
	})
	return err
}

func (s *Storage) MergeRemoteState(indexes []IdxMeta, join bool) {
	lo.ForEach(indexes, func(item IdxMeta, _ int) {
		v, _, err := s.Get(fmt.Sprintf("%s%s", indexKeyPrefix, item.Name))
		var idx IdxMeta
		if err == nil {
			json.Unmarshal([]byte(v), &idx)
			if idx.Version < item.Version {
				s.DropIndex(item.Name)
				s.CreateIndex(item)
			}
		} else {
			s.CreateIndex(item)
		}

	})
}
func NewStorage(name string, replicas, partitions int, logger *log.Logger) (*Storage, error) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		logger.Printf("failed to create db %s \n", err.Error())
		return nil, err
	}
	err = db.Update(func(tx *buntdb.Tx) error {
		return tx.CreateIndex(indexIndexName, fmt.Sprintf("%s*", indexKeyPrefix), buntdb.IndexJSON(indexVersion))
	})
	if err != nil {
		panic(fmt.Sprintf("failed to init database %s", err.Error()))
	}
	s := &Storage{
		name:     name,
		replicas: replicas,
		logger:   logger,
		DB:       db,
		Consistent: consistent.New(nil, consistent.Config{
			PartitionCount: partitions,
			Hasher:         &hash{},
		}),
	}
	return s, nil
}

func (s *Storage) Primary(k string) (*memberlist.Node, error) {
	n, ok := s.LocateKey([]byte(k)).(*memberlist.Node)
	if ok {
		return n, nil
	} else {
		return nil, fmt.Errorf("failed to get the primay cluster of key")
	}
}

func (s *Storage) Replicas(k string) ([]*memberlist.Node, error) {
	var nodes []*memberlist.Node
	members, err := s.GetClosestN([]byte(k), s.replicas)
	if err != nil {
		s.logger.Printf("failed to get replicated cluster of key %s \n", k)
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
