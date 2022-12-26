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
)

func startNodes(num int) []*DB {
	var dbs []*DB
	var existing []string
	for i := 0; i < num; i++ {
		if n, err := NewDB(Options{
			DefaultPort + i,
			existing,
			2,
		}); err != nil {
			log.Fatalf("failed to start the node %s", err.Error())
		} else {
			existing = append(existing, fmt.Sprintf("localhost:%d", DefaultPort+i))
			dbs = append(dbs, n)
		}
	}
	return dbs
}

type DBTestSuite struct {
	suite.Suite
	nodes []*DB
}

func (suite *DBTestSuite) SetupSuite() {
	suite.nodes = startNodes(5)
}

func (suite *DBTestSuite) TearDownSuite() {
	lo.ForEach(suite.nodes, func(n *DB, _ int) {
		n.members.Shutdown()
	})
}

func TestDbSuit(t *testing.T) {
	suite.Run(t, new(DBTestSuite))
}

func (db *DBTestSuite) TestHappyFlow() {
	assert.True(db.T(), len(db.nodes) == 5)
	for _, node := range db.nodes {
		assert.True(db.T(), len(node.members.Members()) == 5)
		assert.True(db.T(), assert.True(db.T(), len(node.members.Members()) == 5))
	}

}

func (db *DBTestSuite) TestSet() {
	db.nodes[0].Set("abc", "123")
	memberNodes, _ := db.nodes[0].storage.Replicas("abc")
	for _, node := range memberNodes {
		fmt.Println(node.Name)
	}
	var cnt int
	for _, m := range memberNodes {
		for _, node := range db.nodes {
			if node.members.LocalNode().Name == m.Name {
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
	db.nodes[0].Set(k, v)
	for _, node := range db.nodes {
		v1, err := node.Get(k)
		require.NoError(db.T(), err)
		require.Equal(db.T(), v, v1)
	}
}
