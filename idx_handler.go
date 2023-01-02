package daos

import (
	"github.com/kcmvp/daos/internal"
	"github.com/vmihailenco/msgpack/v5"
)

var createIndexHandler Handler = func(dc *cluster, cmd Command) {
	var idx internal.IdxMeta
	if err := msgpack.Unmarshal([]byte(cmd.Value), &idx); err == nil {
		if !dc.hasIndex(idx.Name) {
			if err = dc.storage.CreateIndex(idx); err != nil {
				dc.Logger().Printf("error: failed to create index %s: %+v", idx.Name, err.Error())
			} else {
				dc.Logger().Printf("existing index %s in node %s \n", idx.Name, dc.LocalNode())
			}
		}
	} else {
		dc.Logger().Printf("error: failed to parse createIndex command %s", err.Error())
	}
}

var dropIndexHandler Handler = func(dc *cluster, cmd Command) {
	if dc.hasIndex(cmd.Value) {
		if err := dc.storage.DropIndex(cmd.Value); err != nil {
			dc.Logger().Printf("failed to drop index  %s on node %s: %+v", cmd.Value, dc.LocalNode(), err)
		}
	} else {
		dc.Logger().Printf("%s does not have index %s \n", dc.LocalNode(), cmd.Value)
	}
}
