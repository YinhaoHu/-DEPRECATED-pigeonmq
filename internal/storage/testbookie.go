package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"pigeonmq/internal/util"
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
	err = bk.initFilSystem()
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
