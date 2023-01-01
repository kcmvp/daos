package daos

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func startNodes(num int) []DB {
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

type DBTestSuite struct {
	suite.Suite
	nodes []DB
}

func (suite *DBTestSuite) SetupSuite() {
	suite.nodes = startNodes(5)
}

func (suite *DBTestSuite) TearDownSuite() {
	lo.ForEach(suite.nodes, func(db DB, _ int) {
		db.Shutdown()
	})
}

func TestDbSuit(t *testing.T) {
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
		suite.T().Run(test.name, func(t *testing.T) {
			err := test.node.Set(test.key, test.value)
			require.NoError(t, err)
			time.Sleep(500 * time.Microsecond)
			// assert key's existence in internal storage
			lo.ForEach(suite.Replicas(test.key), func(c *cluster, _ int) {
				v1, _, err1 := c.storage.Get(test.key)
				require.NoError(t, err1)
				require.Equal(t, test.value, v1)
				v1, _, err1 = c.Get(test.key)
				require.NoError(t, err1, "should get value from any node")
				require.Equal(t, test.value, v1)
			})
			// assert key should not in none-replicas
			lo.ForEach(suite.NoneReplicas(test.key), func(c *cluster, _ int) {
				v1, _, err1 := c.storage.Get(test.key)
				require.Error(t, err1, "key should not in the none-replicas")
				require.Equal(t, "", v1)
				v1, _, err1 = c.Get(test.key)
				require.NoError(t, err1, "should get value from any node")
				require.Equal(t, test.value, v1)
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
		d.(*cluster).Set(keys[idx], strconv.FormatUint(rand.Uint64(), 10))
	})
	lo.ForEach(suite.nodes, func(d DB, idx int) {
		v, _, err := d.(*cluster).Get(keys[idx])
		require.NoError(suite.T(), err)
		require.NotEmpty(suite.T(), v)
		err = d.(*cluster).Del(keys[idx])
		require.NoError(suite.T(), err)
		v, _, err = d.(*cluster).Get(keys[idx])
		require.Error(suite.T(), err)
	})
}
func (suite *DBTestSuite) TestCreateIndex() {
	suite.nodes[0].CreateJsonIndex(Index{
		Name:     "idx1",
		Bucket:   "abc*",
		JsonPath: []string{"abc.def"},
	})
	time.Sleep(200 * time.Millisecond)
	var ver int64
	for _, node := range suite.nodes {
		c := node.(*cluster)
		indexes, _ := c.storage.Indexes()
		if ver == 0 {
			ver = indexes[0].Version
		}
		require.Equal(suite.T(), ver, indexes[0].Version)
		require.Equal(suite.T(), 1, len(indexes), c.members.LocalNode().Name)
	}
}