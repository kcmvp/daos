package daos

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hashicorp/memberlist"
	"github.com/kcmvp/daos/internal"
	"github.com/samber/lo"
	lop "github.com/samber/lo/parallel"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

type Book struct {
	Name      string `faker:"word,unique"`
	Date      sql.NullTime
	Author    string `fake:"{randomstring:[aa,ab]}"`
	Publisher struct {
		Name string `fake:"{randomstring:[pa,pb]}"`
		City string `fake:"{city}"`
	}
	Price int
}

func (b Book) Key() string {
	return fmt.Sprintf("b:%s", b.Name)
}

func startCluster(num int) []DB {
	var dbs []DB
	var existing []string
	for i := 0; i < num; i++ {
		if n, err := NewDB(Options{
			Port:     DefaultPort + i,
			Nodes:    existing,
			Replicas: 2,
		}); err != nil {
			log.Fatalf("failed to start the cluster %s", err.Error())
		} else {
			existing = append(existing, fmt.Sprintf("0.0.0.0:%d", DefaultPort+i))
			dbs = append(dbs, n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return dbs
}
func startSingle(options ...Options) (DB, error) {
	var opt Options
	if len(options) == 0 {
		opt = Options{
			Replicas: 2,
			Port:     DefaultPort + int(time.Now().Unix()%9999),
		}
	} else {
		opt = options[0]
	}
	return NewDB(opt)
}

type DBTestSuite struct {
	suite.Suite
	nodes []DB
}

func (suite *DBTestSuite) SetupSuite() {
	suite.nodes = startCluster(5)
}

func (suite *DBTestSuite) TearDownSuite() {
	lo.ForEach(suite.nodes, func(db DB, _ int) {
		db.Shutdown()
	})
}

func (suite *DBTestSuite) AfterTest(_, method string) {
	if strings.HasSuffix(method, "Index") {
		lop.ForEach(suite.nodes, func(db DB, _ int) {
			lop.ForEach(db.Indexes(), func(idx Index, _ int) {
				db.DropIndex(idx.Name)
			})
			require.Equal(suite.T(), 0, len(db.Indexes()))
		})
	}
}

func TestDbCluster(t *testing.T) {
	suite.Run(t, new(DBTestSuite))
}

func (suite *DBTestSuite) Primary(key string) *cluster {
	pm, _ := suite.nodes[0].(*cluster).storage.Primary(key)
	pc, _ := lo.Find(suite.nodes, func(item DB) bool {
		return item.(*cluster).LocalNode() == pm.Name
	})
	return pc.(*cluster)
}

func (suite *DBTestSuite) Replicas(key string) []*cluster {
	ns, _ := suite.nodes[0].(*cluster).storage.Replicas(key)
	return lo.FilterMap(ns, func(m *memberlist.Node, _ int) (*cluster, bool) {
		dn, ok := lo.Find(suite.nodes, func(item DB) bool {
			return item.(*cluster).LocalNode() == m.Name
		})
		return dn.(*cluster), ok
	})
}
func (suite *DBTestSuite) NoneReplicas(key string) []*cluster {
	cs := lo.Map(suite.nodes, func(db DB, _ int) *cluster {
		return db.(*cluster)
	})
	return lo.Filter(cs, func(c *cluster, _ int) bool {
		return !lo.Contains(suite.Replicas(key), c)
	})
}

func (suite *DBTestSuite) TestCluster() {
	k := strconv.FormatUint(rand.Uint64(), 10)
	primary := suite.Primary(k)
	replicas := suite.Replicas(k)
	noReplicas := suite.NoneReplicas(k)
	require.Equal(suite.T(), 2, len(replicas))
	require.Equal(suite.T(), 3, len(noReplicas))
	require.Equal(suite.T(), primary, replicas[0])
	require.True(suite.T(), lo.Contains(replicas, primary))
	require.False(suite.T(), lo.Contains(noReplicas, primary))
	require.True(suite.T(), len(lo.Intersect(replicas, noReplicas)) == 0)
	allNodes := lo.Map(suite.nodes, func(db DB, _ int) *cluster {
		return db.(*cluster)
	})
	lo.ForEach(allNodes, func(c *cluster, _ int) {
		_, ok := lo.Find(lo.Union(replicas, noReplicas), func(c1 *cluster) bool {
			return c.LocalNode() == c1.LocalNode()
		})
		require.True(suite.T(), ok)
	})
}

func (d *DBTestSuite) TestTopologyChangesForIndex() {
	c := d.nodes[0].(*cluster)
	index := Index{
		fmt.Sprintf("idx%d", time.Now().Unix()),
		"b:*",
		[]string{"Author"},
	}

	node, _ := startSingle()
	nc := node.(*cluster)
	v1 := time.Now().Unix()
	nc.storage.CreateIndex(internal.IdxMeta{
		index.Name,
		index.Key,
		index.JsonPath,
		v1,
	})
	require.Equal(d.T(), 1, len(node.Indexes()))

	// before join
	idxes, _ := nc.storage.Indexes()
	require.Equal(d.T(), idxes[0].Version, v1)

	// create index in the cluster
	c.CreateJsonIndex(index)
	require.Equal(d.T(), 1, len(c.Indexes()))
	require.Equal(d.T(), 5, len(c.members.Members()))

	// join the cluster
	nc.members.Join([]string{fmt.Sprintf("0.0.0.0:%d", DefaultPort)})
	time.Sleep(100 * time.Microsecond)
	require.Equal(d.T(), 6, len(c.members.Members()))
	require.Equal(d.T(), 1, len(node.Indexes()))

	// after join
	idxes, _ = nc.storage.Indexes()
	require.True(d.T(), idxes[0].Version > v1)
	oidxes, _ := c.storage.Indexes()
	require.Equal(d.T(), idxes[0].Version, oidxes[0].Version)
}

func (suite *DBTestSuite) TestTopologyChangesJoin() {
	tests := []struct {
		name string
		opt  Options
	}{
		{
			name: "inconsistent replicas",
			opt:  Options{Replicas: 3},
		},
		{
			name: "inconsistent partitions",
			opt:  Options{Partitions: 666},
		},
		{
			name: "inconsistent timeout",
			opt:  Options{Timeout: 2 * time.Second},
		},
		{
			name: "inconsistent retry",
			opt:  Options{Retry: 8},
		},
	}
	for _, test := range tests {
		opt := test.opt
		opt.Port = 8080 + int(time.Now().Unix()%99)
		suite.Run(test.name, func() {
			node, err := startSingle(opt)
			require.NoError(suite.T(), err)
			c := node.(*cluster)
			_, err = c.members.Join([]string{fmt.Sprintf("0.0.0.0:%d", DefaultPort)})
			require.Error(suite.T(), err)
			node.Shutdown()
		})
	}
}
func (suite *DBTestSuite) TestSetAndGet() {
	keys := []string{
		fmt.Sprintf("k1%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k2%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k3%s", strconv.FormatUint(rand.Uint64(), 10)),
	}
	tests := []struct {
		name  string
		key   string
		value string
		node  *cluster
	}{
		{
			name:  "Set from primary",
			key:   keys[0],
			value: strconv.FormatUint(rand.Uint64(), 10),
			node:  suite.Primary(keys[0]),
		},
		{
			name:  "Set from replicas",
			key:   keys[1],
			value: strconv.FormatUint(rand.Uint64(), 10),
			node:  suite.Replicas(keys[1])[1],
		},
		{
			name:  "Set from no-replicas",
			key:   keys[2],
			value: strconv.FormatUint(rand.Uint64(), 10),
			node:  lo.Shuffle(suite.NoneReplicas(keys[1]))[0], // random node
		},
	}
	for _, test := range tests {
		suite.Run(test.name, func() {
			err := test.node.Set(test.key, test.value)
			require.NoError(suite.T(), err)
			time.Sleep(500 * time.Microsecond)
			// assert key's existence in internal storage
			lo.ForEach(suite.Replicas(test.key), func(c *cluster, _ int) {
				v1, _, err1 := c.storage.Get(test.key)
				require.NoError(suite.T(), err1)
				require.Equal(suite.T(), test.value, v1)
				v1, _, err1 = c.Get(test.key)
				require.NoError(suite.T(), err1, "should get value from any node")
				require.Equal(suite.T(), test.value, v1)
			})
			// assert key should not in none-replicas
			lo.ForEach(suite.NoneReplicas(test.key), func(c *cluster, _ int) {
				v1, _, err1 := c.storage.Get(test.key)
				require.Error(suite.T(), err1, "key should not in the none-replicas")
				require.Equal(suite.T(), "", v1)
				v1, _, err1 = c.Get(test.key)
				require.NoError(suite.T(), err1, "should get value from any node")
				require.Equal(suite.T(), test.value, v1)
			})
		})
	}
}

func (suite *DBTestSuite) TestSetAndDel() {
	keys := []string{
		fmt.Sprintf("k0%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k1%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k2%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k3%s", strconv.FormatUint(rand.Uint64(), 10)),
		fmt.Sprintf("k4%s", strconv.FormatUint(rand.Uint64(), 10)),
	}
	lo.ForEach(suite.nodes, func(d DB, idx int) {
		if idx%2 == 0 {
			d.(*cluster).SetWithTtl(keys[idx], strconv.FormatUint(rand.Uint64(), 10), time.Minute)
		} else {
			d.(*cluster).Set(keys[idx], strconv.FormatUint(rand.Uint64(), 10))
		}
	})
	time.Sleep(500 * time.Microsecond)
	lo.ForEach(suite.nodes, func(d DB, idx int) {
		v, ttl, err := d.(*cluster).Get(keys[idx])
		require.NoError(suite.T(), err)
		require.NotEmpty(suite.T(), v)
		if idx%2 == 0 {
			require.True(suite.T(), ttl > 0)
		} else {
			require.True(suite.T(), ttl < 0)
		}
		err = d.(*cluster).Del(keys[idx])
		require.NoError(suite.T(), err)
		//time.Sleep(200 * time.Microsecond)
		v, _, err = d.(*cluster).Get(keys[idx])
		if err == nil {
			fmt.Printf("vv: %s \n", v)
		}
		require.Error(suite.T(), err)
	})
}
func (suite *DBTestSuite) TestCreateDropIndex() {
	suite.nodes[0].CreateJsonIndex(Index{
		Name:     "idx1",
		Key:      "abc*",
		JsonPath: []string{"abc.def"},
	})
	time.Sleep(200 * time.Millisecond)
	for _, node := range suite.nodes {
		c := node.(*cluster)
		indexes, _ := c.storage.Indexes()
		ver := indexes[0].Version
		require.Equal(suite.T(), ver, indexes[0].Version)
		require.Equal(suite.T(), 1, len(indexes), c.members.LocalNode().Name)
	}
}

func (suite *DBTestSuite) TestSearchIndex() {
	bookIndex := "bookIndex"
	suite.nodes[0].CreateJsonIndex(Index{
		bookIndex,
		"b:*",
		[]string{"Author"},
	})
	time.Sleep(100 * time.Microsecond)
	lo.ForEach(suite.nodes, func(db DB, _ int) {
		require.Equal(suite.T(), 1, len(db.Indexes()))
	})
	books := lo.Times[Book](20, func(_ int) Book {
		var b Book
		gofakeit.Struct(&b)
		fmt.Println(b)
		return b
	})
	lo.ForEach(books, func(b Book, _ int) {
		str, _ := json.Marshal(b)
		suite.nodes[0].(*cluster).Set(b.Key(), string(str))
	})
	lo.ForEach(books, func(b Book, _ int) {
		v, _, err := suite.nodes[0].Get(b.Key())
		require.NoError(suite.T(), err)
		require.NotEmpty(suite.T(), v)
	})

	gba := lo.GroupBy(books, func(b Book) string {
		return b.Author
	})
	tests := []struct {
		name     string
		index    string
		criteria string
		result   []Book
	}{
		{
			name:     "Author=aa",
			index:    bookIndex,
			criteria: `{"Author":"aa"}`,
			result:   gba["aa"],
		},
		{
			name:     "empty result",
			index:    bookIndex,
			criteria: `{"Author":"cc"}`,
			result:   gba["cc"],
		},
	}

	for _, test := range tests {
		suite.Run(test.name, func() {
			rsm, err := suite.nodes[0].Search(test.index, test.criteria)
			require.NoError(suite.T(), err)
			require.Equal(suite.T(), len(test.result), len(rsm))
		})
	}

}
