package daos

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"strings"
)

type delegate struct {
	db *DB
}

// Create(conf *Config) -> memberlist.setAlive() -> NodeMeta(limit int) ->  m.aliveNode(&a, notifyCh, true) -> config.Alive.NotifyAlive -> encodeBroadcastNotify -> m.config.Events.NotifyJoin(&state.Node)
// UpdateNode(timeout time.Duration) -> NodeMeta(limit int) -> m.aliveNode(&a, notifyCh, true) -> config.Alive.NotifyAlive -> encodeBroadcastNotify -> m.config.Events.NotifyUpdate(&state.Node)
func (d *delegate) NodeMeta(limit int) []byte {
	var indexes []string
	d.db.storage.View(func(tx *buntdb.Tx) error {
		size := 0
		tx.Descend(indexMeta, func(key, val string) bool {
			size += len(val)
			// "[" and "]" use 2 bytes
			if size+len(indexes) < limit-2 {
				indexes = append(indexes, val)
				return true
			}
			return false
		})
		return nil
	})
	if len(indexes) == 0 {
		return []byte{}
	} else {
		return []byte(fmt.Sprintf("[%s]", strings.Join(indexes, ",")))
	}
}

// Create(conf *Config) -> newMemberlist(conf *Config)
// 1: m.handoffCh -> go m.packetHandler() -> packetHandler(** read from net**)-> handleUser(buf []byte, from net.Addr) -> NotifyMsg(buf)
// go m.streamListen() -> m.streamListen() -> streamListen(** read from net **)  -> handleConn(conn net.Conn) -> readUserMsg(bufConn io.Reader, dec *codec.Decoder) -> d.NotifyMsg(userBuf)
func (db *delegate) NotifyMsg(bytes []byte) {
	//TODO implement me
	panic("implement me")
}

// net.handleCommand -> net.handlePing        -\
// net.handleCommand -> net.handleIndirectPing -> net.encodeAndSendMsg -> sendMsg -> getBroadcasts -> *
// memberlist.Create -> state.schedule -> state.gossip -> getBroadcasts -> *
func (db *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	//TODO implement me
	panic("implement me")
}

//		memberlist.Create -> state.schedule -> state.pushPullTrigger -> state.pushPull -> state.pushPull -\
//	                                               (*client)  -> memberlist.Join -> state.pushPullNode -> net.sendAndReceiveState - \
//	                                            memberlist.Create -> memberlist.newMemberlist -> net.streamListen -> net.handleConn -> net.sendLocalState -> *
func (d *delegate) LocalState(join bool) []byte {
	var indexes []string
	d.db.storage.View(func(tx *buntdb.Tx) error {
		size := 0
		tx.Descend(indexMeta, func(key, val string) bool {
			size += len(val)
			// "[" and "]" use 2 bytes
			if join || !join && size+len(indexes) < memberlist.MetaMaxSize-2 {
				indexes = append(indexes, val)
				return true
			}
			return false
		})
		return nil
	})
	if len(indexes) == 0 {
		return []byte{}
	} else {
		return []byte(fmt.Sprintf("[%s]", strings.Join(indexes, ",")))
	}
}

//	                                                               memberlist.Join ->\
//		memberlist.create -> state.schedule -> state.pushPullTrigger -> state.pushPull -> state.pushPullNode -\
//		                  memberlist.Create -> memberlist.newMemberlist -> net.streamListen -> net.handleConn -> net.mergeRemoteState -> *
func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) > 0 {
		var remote []index
		err := json.Unmarshal(buf, &remote)
		if err != nil {
			logger.Error("failed to unmarshal remote index", zap.String("node", d.db.members.LocalNode().Address()),
				zap.String("data", string(buf)))
			return
		}
		//var local []index
		d.db.storage.View(func(tx *buntdb.Tx) error {
			tx.Descend(indexMeta, func(key, value string) bool {
				remote = lo.DropWhile(remote, func(item index) bool {
					return strings.TrimLeft(key, indexMetaPrefix) == item.Name &&
						gjson.Get(value, metaVersion).Int() < item.Version
				})
				return true
			})
			return nil
		})
		d.db.storage.Update(func(tx *buntdb.Tx) error {
			lo.ForEach(remote, func(item index, _ int) {
				data, _ := json.Marshal(item)
				tx.Set(fmt.Sprintf("%s%s", indexMetaPrefix, item.Name), string(data), nil)
			})
			return nil
		})
	}
}
