package storage

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"os"
	"os/exec"
	"path/filepath"
	"pigeonmq/internal/testutil"
	"pigeonmq/internal/util"
	"testing"
)

type TestBookieLocation int32

const (
	TestBookieLocationSingle TestBookieLocation = iota
	TestBookieLocationCluster
)

// TestBookie represents the bookie used for testing.
type TestBookie struct {
	bk       *Bookie
	cfg      *Config
	location TestBookieLocation
}

// NewTestBookie generates an instance for testing bookie.
func NewTestBookie(configPath string, location TestBookieLocation) *TestBookie {
	cfg, err := NewConfig(configPath)
	if err != nil {
		panic(err)
	}

	bk := new(Bookie)
	err = error(nil)

	bk.cfg = cfg
	bk.address = fmt.Sprintf("%s:%d", bk.cfg.IPAddress, bk.cfg.Port)

	// initialize the log.
	logFileBaseName := fmt.Sprintf("bookie-test-%v-%v.log", cfg.Port, os.Getpid())
	logFilePath := filepath.Join(cfg.LogFilePath, logFileBaseName)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	bk.logger = util.NewLogger(logFile, util.DebugLevel)

	// initialize the event handlers.
	bk.eventCh = make(chan *Event, maxEventBackLog)

	// initialize the file system related fields of this bookie.
	err = bk.initFileSystem()
	if err != nil {
		panic(err)
	}

	if location == TestBookieLocationCluster {
		// initialize the zookeeper information of this bookie.
		err = bk.initZK()
		if err != nil {
			panic(err)
		}

		// initialize the rpc.
		err = bk.initRPC()
		if err != nil {
			panic(err)
		}
	}

	bk.setState(StateRunning)
	tb := new(TestBookie)
	tb.location = location
	tb.cfg = cfg
	tb.bk = bk
	return tb
}

// testBookieCluster represents a test cluster of bookies used for testing purposes.
type testBookieCluster struct {
	bookieNames []string   // Names of the bookies in the cluster.
	t           *testing.T // Reference to the testing.T instance for reporting errors.
}

// newTestBookieCluster creates a new instance of testBookieCluster with the given bookie names.
func newTestBookieCluster(t *testing.T, names ...string) *testBookieCluster {
	c := &testBookieCluster{
		bookieNames: names,
		t:           t,
	}
	return c
}

// batchProcess runs a batch process on the bookies in the cluster with the given option.
func (c *testBookieCluster) batchProcess(option string) {
	// Iterate over the bookie names and run the process on each bookie.
	for _, name := range c.bookieNames {
		c.runBookieSh(name, option)
	}
}

func (c *testBookieCluster) runBookieSh(name string, option string) {
	bookieShPath, err := filepath.Abs("../../cmd/pigeonmq-bookie/bookie.sh")
	testutil.CheckErrorAndFatalAsNeeded(err, c.t)

	err = exec.Command(bookieShPath, name, option).Run()
	testutil.CheckErrorAndFatalAsNeeded(err, c.t)
}

func (c *testBookieCluster) setupOne(name string) {
	c.runBookieSh(name, "start")
}

// setupAll starts all bookies in the cluster.
func (c *testBookieCluster) setupAll() {
	c.batchProcess("start")
}

func (c *testBookieCluster) tearDownOne(name string) {
	c.runBookieSh(name, "stop")
}

// teardownAll stops all bookies in the cluster.
func (c *testBookieCluster) teardownAll() {
	c.batchProcess("close")
}

// ZKDeleteAll implements the `delete-all` command in zk client.
func ZKDeleteAll(conn *zk.Conn, path string) error {
	children, _, err := conn.Children(path)
	if err != nil {
		return fmt.Errorf("zk children err(%v): %w", path, err)
	}

	for _, child := range children {
		childPath := path + "/" + child
		if err := ZKDeleteAll(conn, childPath); err != nil {
			return err
		}
	}

	err = conn.Delete(path, -1)
	if err != nil && !errors.Is(err, zk.ErrNoNode) {
		return fmt.Errorf("zk delete err(%v): %w", path, err)
	}

	return nil
}
