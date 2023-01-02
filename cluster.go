package daos

import (
	"errors"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type action int

const (
	set action = iota
	del
	get
	getResp
	search
	searchResp
	createIndex
	dropIndex
)

const DefaultPort = 7080

type Command struct {
	Action action        `json:"a,omitempty"`
	Key    string        `json:"k,omitempty"`
	Value  string        `json:"v,omitempty"`
	TTL    time.Duration `json:"t,omitempty"`
	Seq    uint32        `json:"s,omitempty"`
	Caller string        `json:"c,omitempty"`
	Index  string        `json:"i,omitempty"`
	Error  string        `json:"e,omitempty"`
}

type Handler func(dc *cluster, cmd Command)

var handlers = map[action]Handler{
	set:         setHandler,
	del:         delHandler,
	get:         getHandler,
	getResp:     getRespHandler,
	search:      setHandler,
	searchResp:  searchRespHandler,
	createIndex: createIndexHandler,
	dropIndex:   dropIndexHandler,
}

type cluster struct {
	storage    *internal.Storage
	members    *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	options    Options
	chanMap    sync.Map
}

func (dc *cluster) Logger() *log.Logger {
	return dc.options.Logger
}
func (dc *cluster) Timeout() time.Duration {
	return dc.options.Timeout
}
func (dc *cluster) Shutdown() {
	dc.members.Shutdown()
}

func (dc *cluster) NodeMeta(limit int) []byte {
	indexes, size := dc.storage.Indexes()
	idxes := lo.DropRightWhile(indexes, func(item internal.IdxMeta) bool {
		if size > limit {
			t, _ := msgpack.Marshal(item)
			size -= len(t)
			return true
		} else {
			return false
		}
	})
	data, _ := msgpack.Marshal(idxes)
	return data
}

func (dc *cluster) NotifyMsg(bytes []byte) {
	cmd := Command{}
	if err := msgpack.Unmarshal(bytes, &cmd); err != nil {
		dc.options.Logger.Printf("incorrect user msg %s \n", err.Error())
		return
	}
	handler, _ := handlers[cmd.Action]
	handler(dc, cmd)
}

func (dc *cluster) NodeByName(name string) (*memberlist.Node, error) {
	node, ok := lo.Find(dc.members.Members(), func(node *memberlist.Node) bool {
		return node.Name == name
	})
	if ok {
		return node, nil
	} else {
		return nil, fmt.Errorf("can not find the dc %s", name)
	}
}

func (dc *cluster) GetBroadcasts(overhead, limit int) [][]byte {
	return dc.broadcasts.GetBroadcasts(overhead, limit)
}

func (dc *cluster) LocalState(join bool) []byte {

	indexes, _ := dc.storage.Indexes()
	data, _ := msgpack.Marshal(indexes)
	return data
}

func (dc *cluster) MergeRemoteState(buf []byte, join bool) {
	var indexes []internal.IdxMeta
	if err := msgpack.Unmarshal(buf, &indexes); err != nil {
		dc.Logger().Printf("failed to unmarshal index data %s \n", err.Error())
		return
	}
	dc.storage.MergeRemoteState(indexes, join)
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

func (dc *cluster) LocalNode() string {
	return dc.members.LocalNode().Name
}

func (dc *cluster) Replicas(key string) bool {
	nodes, _ := dc.storage.Replicas(key)
	_, ok := lo.Find(nodes, func(n *memberlist.Node) bool {
		return n.Name == dc.LocalNode()
	})
	return ok
}

func (dc *cluster) Primary(key string) bool {
	n, _ := dc.storage.Primary(key)
	return n.Name == dc.LocalNode()
}

func (dc *cluster) Set(k, v string) error {
	return dc.SetWithTtl(k, v, -1*time.Second)
}

// SetWithTtl always set primary cluster first and then write to the replicas directly.
func (dc *cluster) SetWithTtl(k, v string, ttl time.Duration) error {
	err := lo.Validate(!lo.ContainsBy(internal.ReversedBucket(), func(s string) bool {
		return strings.HasPrefix(k, s)
	}), "key starts with reserved prefix %s", k)
	if err != nil {
		return err
	}
	cmd := Command{Action: set, Key: k, Value: v, TTL: ttl}
	data, err := msgpack.Marshal(cmd)
	if err != nil {
		return err
	}
	replicas, _ := dc.storage.Replicas(k)
	// make sure write to primary first
	if dc.Primary(k) {
		if err = dc.storage.Set(k, v, ttl); err == nil {
			lop.ForEach(replicas[1:], func(node *memberlist.Node, _ int) {
				lo.AttemptWithDelay(3, dc.options.Timeout, func(i int, t time.Duration) error {
					err = dc.members.SendBestEffort(node, data)
					if err != nil && i+1 < 3 {
						dc.Logger().Printf("failed to send data to node %s, retry: %d \n", node.Name, i)
					}
					return err
				})
			})
		}
	} else {
		err = dc.members.SendBestEffort(replicas[0], data)
	}
	return err
}

func (dc *cluster) newChan(chanId uint32, buffer ...int) {
	size := 0
	if len(buffer) > 0 && buffer[0] > 0 {
		size = buffer[0]
	}
	dc.chanMap.Store(chanId, make(chan Command, size))
}
func (dc *cluster) getChan(chanId uint32) (chan Command, bool) {
	ch, ok := dc.chanMap.Load(chanId)
	return lo.Ternary(ok, ch.(chan Command), nil), ok
}
func (dc *cluster) delChan(id uint32) {
	dc.chanMap.Delete(id)
}

func (dc *cluster) Get(k string) (string, time.Duration, error) {
	v, ttl, err := dc.storage.Get(k)
	// return the value directly when current node is a replica
	if dc.Replicas(k) {
		return v, ttl, err
	} else {
		cmd := Command{Action: get, Key: k, Seq: rand.Uint32(), Caller: dc.LocalNode()}
		data, _ := msgpack.Marshal(cmd)
		primary, _ := dc.storage.Primary(k)
		dc.newChan(cmd.Seq, 1)
		go func() {
			dc.members.SendBestEffort(primary, data)
		}()
		return func(c Command) (string, time.Duration, error) {
			ch, _ := dc.getChan(c.Seq)
			r, _, t, ok := lo.BufferWithTimeout(ch, 2, dc.Timeout())
			dc.delChan(c.Seq)
			if ok {
				dc.Logger().Printf("error: redirect times out %d microseconds\n ", t/time.Microsecond)
				close(ch)
			} else {
				dc.Logger().Printf("redirect elapse %d microseconds", t/time.Microsecond)
			}
			if len(r) > 0 {
				var e error
				if len(r[0].Error) > 0 {
					e = fmt.Errorf(r[0].Error)
				}
				return r[0].Value, r[0].TTL, e
			} else {
				return "", -1, fmt.Errorf("not found")
			}
		}(cmd)
	}
}

func (dc *cluster) Del(k string) (err error) {
	cmd := Command{Action: del, Key: k}
	data, _ := msgpack.Marshal(cmd)
	replicas, _ := dc.storage.Replicas(k)
	lop.ForEach(replicas, func(node *memberlist.Node, _ int) {
		_, _, err = lo.AttemptWithDelay(3, dc.Timeout(), func(tries int, time time.Duration) error {
			if node.Name == dc.LocalNode() {
				_, err = dc.storage.Del(k)
			} else {
				err = dc.members.SendBestEffort(node, data)
			}
			return err
		})
	})
	return
}

func (dc *cluster) Search(index, criteria string) (map[string]string, error) {
	// send search request to the dc
	cmd := Command{
		Action: search,
		Index:  index,
		Value:  criteria,
		Caller: dc.LocalNode(),
		Seq:    rand.Uint32()}
	data, _ := msgpack.Marshal(&cmd)
	dc.newChan(cmd.Seq, dc.members.NumMembers())
	lop.ForEach(dc.members.Members(), func(r *memberlist.Node, _ int) {
		if r.Name != dc.LocalNode() {
			dc.members.SendBestEffort(r, data)
		}
	})
	// get the result
	return func(c Command) (map[string]string, error) {
		ch, _ := dc.getChan(c.Seq)
		remote, l, _, ok := lo.BufferWithTimeout(ch, dc.members.NumMembers()-1, dc.Timeout())
		dc.delChan(c.Seq)
		if ok {
			close(ch)
		}
		if l != dc.members.NumMembers()-1 {
			return map[string]string{}, errors.New("can't get response from some nodes")
		}
		// query local
		local := dc.storage.SearchIndex(c.Index, c.Value)
		t3s := lo.Map(local, func(r internal.Row, _ int) lo.Tuple3[string, string, string] {
			return lo.T3(dc.LocalNode(), r.Key, r.Value)
		})
		lop.ForEach(remote, func(item Command, _ int) {
			if len(item.Value) > 0 {
				var rows []internal.Row
				msgpack.Unmarshal([]byte(item.Value), &rows)
				a := lop.Map(rows, func(r internal.Row, _ int) lo.Tuple3[string, string, string] {
					return lo.T3(item.Key, r.Key, r.Value)
				})
				t3s = append(t3s, a...)
			}
		})

		groups := lop.GroupBy(t3s, func(t3 lo.Tuple3[string, string, string]) int {
			node, _ := dc.storage.Primary(t3.B)
			if node.Name == t3.A {
				return 1
			} else {
				return 0
			}
		})
		if len(groups) != 2 || len(groups[0]) != len(groups[1]) {
			dc.Logger().Printf("error: data lost in replicas")
			return map[string]string{}, errors.New("data lost in replicas")
		}
		return lo.SliceToMap(groups[1], func(t3 lo.Tuple3[string, string, string]) (string, string) {
			return t3.B, t3.C
		}), nil
	}(cmd)
}

func (dc *cluster) hasIndex(index string) bool {
	indexes, _ := dc.storage.Indexes()
	return lo.ContainsBy(indexes, func(idx internal.IdxMeta) bool {
		return idx.Name == index
	})
}
func (dc *cluster) createJsonIndex(index internal.IdxMeta) error {
	data, _ := msgpack.Marshal(index)
	c := Command{
		Action: createIndex,
		Value:  string(data),
	}
	data, _ = msgpack.Marshal(c)
	var failedNodes []string
	lo.ForEach(dc.members.Members(), func(m *memberlist.Node, _ int) {
		var err error
		lo.AttemptWithDelay(3, dc.Timeout(), func(n int, t time.Duration) error {
			if m.Name == dc.LocalNode() {
				if !dc.hasIndex(index.Name) {
					err = dc.storage.CreateIndex(index)
				} else {
					dc.Logger().Printf("index %s exists, broadcast to other nodes\n", index.Name)
				}
			} else {
				err = dc.members.SendBestEffort(m, data)
			}
			return err
		})
		if err != nil {
			dc.Logger().Printf("failed to create index %s on node %s \n", index.Name, m.Name)
			failedNodes = append(failedNodes, m.Name)
		}
	})
	return lo.Ternary[error](len(failedNodes) > 0, fmt.Errorf("failed to create index %s : %+v", index.Name, failedNodes), nil)
}
func (dc *cluster) CreateJsonIndex(index Index) error {
	if err := index.Validate(); err != nil {
		return err
	}
	return dc.createJsonIndex(internal.IdxMeta{
		Name:     index.Name,
		Key:      index.Key,
		JsonPath: index.JsonPath,
		Version:  time.Now().UnixMilli(),
	})
}

func (dc *cluster) DropIndex(name string) error {
	c := Command{
		Action: dropIndex,
		Value:  name,
	}
	msg, _ := msgpack.Marshal(c)
	var errs []error
	lop.ForEach(dc.members.Members(), func(m *memberlist.Node, _ int) {
		var err error
		if m.Name == dc.LocalNode() {
			if dc.hasIndex(name) {
				err = dc.storage.DropIndex(name)
			}
		} else {
			_, _, err = lo.AttemptWithDelay(3, dc.Timeout(), func(n int, t time.Duration) error {
				if err = dc.members.SendBestEffort(m, msg); err != nil {
					dc.Logger().Printf("drip index %s, retry %d \n", name, n)
					return err
				}
				return nil
			})
		}
		if err != nil {
			dc.Logger().Printf("error: failed to drop index %s on node %s \n", name, dc.LocalNode())
			errs = append(errs, err)
		}
	})
	if len(errs) == 0 {
		return nil
	} else {
		return fmt.Errorf("failed to drop index %s", name)
	}
}

func (dc *cluster) Indexes() []Index {
	idxes, _ := dc.storage.Indexes()
	return lo.Map(idxes, func(idx internal.IdxMeta, _ int) Index {
		return Index{Name: idx.Name, Key: idx.Key, JsonPath: idx.JsonPath}
	})
}

var _ DB = (*cluster)(nil)
var _ memberlist.Broadcast = (*broadcast)(nil)
var _ memberlist.Delegate = (*cluster)(nil)
