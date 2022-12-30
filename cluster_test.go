package daos

import (
	"fmt"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
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

func (db *DBTestSuite) TestHappyFlow() {
	assert.True(db.T(), len(db.nodes) == 5)
	for _, node := range db.nodes {
		c := node.(*cluster)
		assert.True(db.T(), len(c.members.Members()) == 5)
		assert.True(db.T(), assert.True(db.T(), len(c.members.Members()) == 5))
	}
}

func (db *DBTestSuite) TestCreateIndex() {
	db.nodes[0].CreateJsonIndex(Index{
		Name:     "idx1",
		Bucket:   "abc*",
		JsonPath: []string{"abc.def"},
	})
	time.Sleep(200 * time.Millisecond)
	var ver int64
	for _, node := range db.nodes {
		c := node.(*cluster)
		indexes, _ := c.storage.Indexes()
		if ver == 0 {
			ver = indexes[0].Version
		}
		require.Equal(db.T(), ver, indexes[0].Version)
		require.Equal(db.T(), 1, len(indexes), c.members.LocalNode().Name)
	}
}

func (db *DBTestSuite) TestSet() {
	err := db.nodes[0].Set("abc", "123")
	require.NoError(db.T(), err)
	c := db.nodes[0].(*cluster)
	memberNodes, _ := c.storage.Replicas("abc")
	var cnt int
	for _, m := range memberNodes {
		for _, node := range db.nodes {
			c = node.(*cluster)
			if c.members.LocalNode().Name == m.Name {
				v, _ := node.Get("abc")
				assert.Equal(db.T(), "123", v)
				cnt++
				break
			}
		}
	}
	assert.Equal(db.T(), 2, cnt)
}

func (db *DBTestSuite) TestSetAndGetAny() {
	k := strconv.FormatUint(rand.Uint64(), 10)
	v := strconv.Itoa(rand.Int())
	err := db.nodes[0].Set(k, v)
	require.NoError(db.T(), err)
	replicas := 0
	nonReplicas := 0
	for _, node := range db.nodes {
		c := node.(*cluster)
		if c.Replicas(k) {
			replicas++
			// get value from newStorage directly
			v1, err := c.storage.Get(k)
			require.NoError(db.T(), err)
			require.Equal(db.T(), v, v1)
		} else {
			// get value from cluster
			nonReplicas++
			v1, err := node.Get(k)
			require.NoError(db.T(), err)
			require.Equal(db.T(), v, v1)
		}
	}
	require.Equal(db.T(), 2, replicas)
	require.Equal(db.T(), 3, nonReplicas)
}
