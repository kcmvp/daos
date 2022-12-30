package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	jsoniter "github.com/json-iterator/go"
	"github.com/kcmvp/daos/internal"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"log"
	"math/rand"
	"strings"
	"time"
)

const DefaultPort = 7080

type action int

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	set action = iota
	del
	get
	search
	res
	createIndex
	dropIndex
)

type Command struct {
	Action action        `json:"a,omitempty"`
	Key    string        `json:"k,omitempty"`
	Value  string        `json:"v,omitempty"`
	TTL    time.Duration `json:"t,omitempty"`
	Seq    uint32        `json:"s,omitempty"`
	Caller string        `json:"c,omitempty"`
	Index  string        `json:"i,omitempty"`
}

type cluster struct {
	storage    *internal.Storage
	members    *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	logger     *log.Logger
}

func (dc *cluster) Shutdown() {
	dc.members.Shutdown()
}

func (dc *cluster) Ttl(k string) (time.Duration, error) {
	return dc.storage.Ttl(k)
}

func (dc *cluster) NodeMeta(limit int) []byte {
	if dc.members != nil {
		fmt.Printf("**NodeMeta %s\n", dc.members.LocalNode().Name)
	}
	indexes, size := dc.storage.Indexes()
	idxes := lo.DropRightWhile(indexes, func(item internal.IdxMeta) bool {
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

func (dc *cluster) NotifyMsg(bytes []byte) {
	fmt.Printf("**NotifyMsg %s\n\n", dc.members.LocalNode().Name)
	cmd := Command{}
	if err := json.Unmarshal(bytes, &cmd); err != nil {
		dc.logger.Printf("incorrect user msg %s \n", err.Error())
		return
	}
	switch cmd.Action {
	case set:
		//if dc.Primary(cmd.Key) {
		//	dc.SetWithTtl(cmd.Key, cmd.Value, cmd.TTL)
		//} else if dc.Replicas(cmd.Key) {
		//	dc.storage.Set(cmd.Key, cmd.Value, cmd.TTL)
		//}
		if dc.Replicas(cmd.Key) {
			dc.storage.Set(cmd.Key, cmd.Value, cmd.TTL)
		}
	case del:
		//if dc.Primary(cmd.Key) {
		//	dc.Del(cmd.Key)
		//} else if dc.Replicas(cmd.Key) {
		//	dc.storage.Del(cmd.Key)
		//}
		dc.storage.Del(cmd.Key)
	case get:
		v, err := dc.storage.Get(cmd.Key)
		if err != nil {
			return
		}
		caller, err := dc.Node(cmd.Caller)
		if err != nil {
			dc.logger.Printf("can not find the key %s \n", err.Error())
			return
		}
		ttl, err := dc.Ttl(cmd.Key)
		if err != nil {
			dc.logger.Printf("%s \n", err.Error())
			return
		}
		cmd = Command{
			Action: res,
			Key:    cmd.Key,
			Value:  v,
			Seq:    cmd.Seq,
			TTL:    ttl,
		}
		data, _ := json.Marshal(cmd)
		dc.members.SendReliable(caller, data)
	case search:
		resp := dc.storage.SearchIndex(cmd.Index, cmd.Value)
		data := []byte("")
		if len(resp) > 0 {
			data, _ = json.Marshal(resp)
		}
		command := Command{
			Action: res,
			Key:    dc.members.LocalNode().Name,
			Value:  string(data),
			Seq:    cmd.Seq,
			Index:  cmd.Index,
		}
		data, _ = json.Marshal(command)
		caller, _ := dc.Node(cmd.Caller)
		dc.members.SendReliable(caller, data)
	case res:
		// keep the response 5 time.Millisecond
		dc.storage.SetRemote(cmd.Seq, cmd.Key, cmd.Value, 5*time.Millisecond)
		// no index means it's a remote get request
		// in case current dc is a replica of the key
		if len(cmd.Index) < 1 && dc.Replicas(cmd.Key) {
			dc.storage.Set(cmd.Key, cmd.Value, cmd.TTL)
		}
	case createIndex:
		var idx internal.IdxMeta
		err := json.Unmarshal([]byte(cmd.Value), &idx)
		if err == nil {
			dc.createJsonIndex(idx)
		} else {
			fmt.Println(err.Error())
		}
	case dropIndex:
		dc.DropIndex(cmd.Value)
	}
}
func (dc *cluster) Node(name string) (*memberlist.Node, error) {
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
	//fmt.Printf("**GetBroadcasts %s\dc", dc.members.LocalNode().Name)
	return dc.broadcasts.GetBroadcasts(overhead, limit)
}

func (dc *cluster) LocalState(join bool) []byte {
	if dc.members != nil {
		fmt.Printf("**LocalState %s\n", dc.members.LocalNode().Name)
	}
	indexes, _ := dc.storage.Indexes()
	data, _ := json.Marshal(indexes)
	return data
}

func (dc *cluster) MergeRemoteState(buf []byte, join bool) {
	if dc.members != nil {
		fmt.Printf("**MergeRemoteState %s\n", dc.members.LocalNode().Name)
	}
	dc.storage.MergeRemoteState(buf, join)
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

func (dc *cluster) Set(k, v string) error {
	return dc.SetWithTtl(k, v, 0*time.Second)
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
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// make sure write to primary first
	if dc.Primary(k) {
		err = dc.storage.Set(k, v, ttl)
		if err != nil {
			return err
		}
	}
	replicas, _ := dc.storage.Replicas(k)
	lop.ForEach(replicas, func(node *memberlist.Node, _ int) {
		if node.Name != dc.members.LocalNode().Name {
			dc.members.SendBestEffort(node, data)
		}
	})
	return nil
}

func (dc *cluster) Replicas(key string) bool {
	nodes, _ := dc.storage.Replicas(key)
	_, ok := lo.Find(nodes, func(n *memberlist.Node) bool {
		return n.Name == dc.members.LocalNode().Name
	})
	return ok
}

func (dc *cluster) Primary(key string) bool {
	node, _ := dc.storage.Primary(key)
	return node.Name == dc.members.LocalNode().Name
}

func (dc *cluster) Get(k string) (string, error) {
	v, err := dc.storage.Get(k)
	if err == nil {
		return v, nil
	} else {
		cmd := Command{Action: get, Key: k, Seq: rand.Uint32(), Caller: dc.members.LocalNode().Name}
		data, _ := json.Marshal(cmd)
		primary, _ := dc.storage.Primary(k)
		// redirect to the primary node
		go func() {
			dc.members.SendBestEffort(primary, data)
		}()
		return func(c Command) (string, error) {
			tries, times, _ := lo.AttemptWithDelay(30, 100*time.Microsecond, func(_ int, d time.Duration) error {
				v, err = dc.storage.GetRemote(c.Seq, c.Key)
				return err
			})
			if err == nil {
				dc.logger.Printf("redirect %d, times %d", tries, times/time.Microsecond)
			}
			return v, err
		}(cmd)
	}
}

func (dc *cluster) Search(index, criteria string) (map[string]string, error) {
	// send ipc_search request to the dc
	cmd := Command{
		Action: search,
		Index:  index,
		Value:  criteria,
		Caller: dc.members.LocalNode().Name,
		Seq:    rand.Uint32()}
	data, _ := json.Marshal(&cmd)
	lop.ForEach(dc.members.Members(), func(r *memberlist.Node, _ int) {
		if r.Name != dc.members.LocalNode().Name {
			dc.members.SendReliable(r, data)
		}
	})
	// get the result
	return func(c Command) (map[string]string, error) {
		local := dc.storage.SearchIndex(c.Index, c.Value)
		var primaries []internal.Response
		var replicas []internal.Response
		_, _, err := lo.AttemptWithDelay(6, 300*time.Microsecond, func(n int, d time.Duration) error {
			packedrs := dc.storage.ScanIndexRemote(c.Seq)
			if len(packedrs)+1 != len(dc.members.Members()) {
				return fmt.Errorf("try %d", n)
			}
			for _, pks := range packedrs {
				var rs []internal.Response
				json.Unmarshal([]byte(pks.Value), &rs)
				node, _ := lo.Last(strings.Split(pks.Key, ":"))
				for _, r := range rs {
					p, _ := dc.storage.Primary(r.Key)
					if node == p.Name {
						primaries = append(primaries, r)
					} else {
						replicas = append(replicas, r)
					}
				}
			}
			return nil
		})
		if err != nil {
			return map[string]string{}, err
		}
		// update the missing key-value
		go func() {
			remote := append(primaries, replicas...)
			var processed []string
			lo.ForEach(remote, func(r internal.Response, _ int) {
				if dc.Replicas(r.Key) && !lo.ContainsBy(local, func(l internal.Response) bool {
					return r.Key == l.Key
				}) && !lo.ContainsBy(processed, func(p string) bool {
					return r.Key == p
				}) {
					processed = append(processed, r.Key)
				}
			})
		}()

		// 2: always get the value from primary dc
		rt := lo.FilterMap(local, func(l internal.Response, _ int) (internal.Response, bool) {
			return l, !lo.ContainsBy(primaries, func(i internal.Response) bool {
				return true
			})
		})
		primaries = append(primaries, rt...)
		rt = lo.FilterMap(replicas, func(l internal.Response, _ int) (internal.Response, bool) {
			return l, !lo.ContainsBy(primaries, func(i internal.Response) bool {
				return true
			})
		})
		primaries = append(primaries, rt...)
		var m map[string]string
		lo.ForEach(primaries, func(i internal.Response, _ int) {
			m[i.Key] = i.Value
		})
		return m, err
	}(cmd)
}

func (dc *cluster) Del(k string) {
	cmd := Command{Action: del, Key: k}
	data, _ := json.Marshal(cmd)
	replicas, _ := dc.storage.Replicas(k)
	lop.ForEach(replicas, func(node *memberlist.Node, _ int) {
		if node.Name != dc.members.LocalNode().Name {
			dc.members.SendReliable(node, data)
		} else {
			dc.storage.Del(k)
		}
	})
}

func (dc *cluster) createJsonIndex(index internal.IdxMeta) error {

	indexes, _ := dc.storage.Indexes()
	if lo.ContainsBy(indexes, func(idx internal.IdxMeta) bool {
		return idx.Name == index.Name
	}) {
		return fmt.Errorf("existing index name %s", index.Name)
	}
	err := dc.storage.CreateIndex(index)
	if err != nil {
		return err
	}
	data, _ := json.Marshal(index)
	c := Command{
		Action: createIndex,
		Value:  string(data),
	}
	data, _ = json.Marshal(c)
	dc.broadcasts.QueueBroadcast(&broadcast{
		msg:    data,
		notify: nil,
	})
	if err != nil {
		dc.logger.Printf("failed to create index %s \n", err.Error())
	}
	return err

}
func (dc *cluster) CreateJsonIndex(index Index) error {
	if err := index.Validate(); err != nil {
		return err
	}
	return dc.createJsonIndex(internal.IdxMeta{
		Name:     index.Name,
		Bucket:   index.Bucket,
		JsonPath: index.JsonPath,
		Version:  time.Now().UnixMilli(),
	})
}

func (dc *cluster) DropIndex(name string) error {
	err := dc.storage.DropIndex(name)
	if err != nil {
		return err
	}
	c := Command{
		Action: dropIndex,
		Value:  name,
	}
	msg, _ := json.Marshal(c)
	dc.broadcasts.QueueBroadcast(&broadcast{
		msg:    msg,
		notify: nil,
	})
	return err
}

var _ DB = (*cluster)(nil)
var _ memberlist.Broadcast = (*broadcast)(nil)
var _ memberlist.Delegate = (*cluster)(nil)
