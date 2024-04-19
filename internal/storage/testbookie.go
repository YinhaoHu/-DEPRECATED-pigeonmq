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
	"time"
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
func NewTestBookie(cfg *Config, location TestBookieLocation) *TestBookie {
	bk := new(Bookie)
	err := error(nil)

	bk.cfg = cfg
	bk.address = fmt.Sprintf("%s:%d", bk.cfg.IPAddress, bk.cfg.Port)

	// initialize the log.
	logFileBaseName := fmt.Sprintf("bookie-test-%v-%v-%v.log", cfg.Port, os.Getpid(), time.Now().UnixNano())
	logFilePath := filepath.Join(cfg.LogFilePath, logFileBaseName)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	bk.logger = util.NewLogger(logFile, util.DebugLevel)

	// allocate memory for fields
	bk.eventCh = make(chan *Event, maxEventBackLog)
	bk.segments = make(map[string]*segment)
	bk.openSegmentFiles = make(map[string]*os.File)

	bk.recovery()
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

type TestBookieCluster struct {
	Bookies []*TestBookie
	Testing *testing.T
}

func NewTestBookieCluster(t *testing.T) *TestBookieCluster {
	return &TestBookieCluster{
		Bookies: make([]*TestBookie, 0),
		Testing: t,
	}
}

func (c *TestBookieCluster) runBookieSh(name string, option string) {
	bookieShPath, err := filepath.Abs("../../cmd/pigeonmq-bookie/bookie.sh")
	testutil.CheckErrorAndFatalAsNeeded(err, c.Testing)

	err = exec.Command(bookieShPath, name, option).Run()
	testutil.CheckErrorAndFatalAsNeeded(err, c.Testing)
}

// Join adds a bookie with cfgTemplate and port into the cluster without starting by default.
// port should be this format: 1900*, e.g: 19001, 19002.
func (c *TestBookieCluster) Join(cfgTemplate *Config, port int) {
	cfg := *cfgTemplate
	cfg.Port = port
	storageDirPwd, storageDir := filepath.Split(cfgTemplate.StorageDirectoryPath)
	storageDir = fmt.Sprintf("storage%v", port%19000)
	cfg.StorageDirectoryPath = filepath.Join(storageDirPwd, storageDir)
	newTestBookie := NewTestBookie(&cfg, TestBookieLocationCluster)
	c.Bookies = append(c.Bookies, newTestBookie)
}

func (c *TestBookieCluster) TearDownAll() {
	for _, tb := range c.Bookies {
		tb.bk.Shutdown()
	}
}

func (c *TestBookieCluster) StartAll() {
	for _, tb := range c.Bookies {
		go tb.bk.Run()
	}
}

func (c *TestBookieCluster) TearDownOne(id int) {
	c.Bookies[id].bk.Shutdown()
}

func (c *TestBookieCluster) StartOne(id int) {
	c.Bookies[id] = NewTestBookie(c.Bookies[id].cfg, c.Bookies[id].location)
	go c.Bookies[id].bk.Run()
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
