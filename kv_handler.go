package daos

import (
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"github.com/vmihailenco/msgpack/v5"
	"time"
)

var getHandler Handler = func(dc *cluster, cmd Command) {
	v, ttl, err := dc.storage.Get(cmd.Key)
	getRes := Command{
		Action: getResp,
		Key:    cmd.Key,
		Value:  v,
		Seq:    cmd.Seq,
		TTL:    ttl,
	}
	if err != nil {
		getRes.Error = err.Error()
	}
	caller, err := dc.NodeByName(cmd.Caller)
	if err != nil {
		dc.Logger().Printf("can not find the node %s: %s \n", cmd.Caller, err.Error())
		return
	}
	data, _ := msgpack.Marshal(getRes)
	dc.members.SendReliable(caller, data)
}

var getRespHandler Handler = func(dc *cluster, cmd Command) {
	if ch, ok := dc.getChan(cmd.Seq); ok {
		ch <- Command{Value: cmd.Value, TTL: cmd.TTL, Error: cmd.Error}
		close(ch)
		// in case current dc is a replica of the key
		if dc.Replicas(cmd.Key) && cmd.Error == "" {
			dc.storage.Set(cmd.Key, cmd.Value, cmd.TTL)
		}
	}

}

var setHandler Handler = func(dc *cluster, cmd Command) {
	if dc.Replicas(cmd.Key) {
		msg, _ := msgpack.Marshal(cmd)
		if err := dc.storage.Set(cmd.Key, cmd.Value, cmd.TTL); err == nil && dc.Primary(cmd.Key) {
			// only issue set from primary
			replicas, _ := dc.storage.Replicas(cmd.Key)
			lop.ForEach(replicas[1:], func(n *memberlist.Node, _ int) {
				lo.AttemptWithDelay(dc.options.Retry, dc.options.Timeout, func(try int, time time.Duration) error {
					return dc.members.SendBestEffort(n, msg)
				})
			})
		}
	}
}

var delHandler Handler = func(dc *cluster, cmd Command) {
	if _, err := dc.storage.Del(cmd.Key); err != nil {
		dc.Logger().Printf("failed to delete the key %s: %s \n", cmd.Key, err.Error())
	}
}

var searchHandler Handler = func(dc *cluster, cmd Command) {
	rows := dc.storage.SearchIndex(cmd.Index, cmd.Value)
	data, _ := msgpack.Marshal(rows)
	command := Command{
		Action: searchResp,
		Key:    dc.members.LocalNode().Name,
		Value:  string(data),
		Seq:    cmd.Seq,
	}
	msg, _ := msgpack.Marshal(command)
	caller, _ := dc.NodeByName(cmd.Caller)
	dc.members.SendReliable(caller, msg)
}

var searchRespHandler Handler = func(dc *cluster, cmd Command) {
	if ch, ok := dc.getChan(cmd.Seq); ok {
		ch <- Command{Key: cmd.Key, Value: cmd.Value}
	}
}
