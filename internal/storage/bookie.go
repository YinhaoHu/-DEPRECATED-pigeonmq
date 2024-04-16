package storage

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"io/fs"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"pigeonmq/internal/util"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type State int32

const (
	StateUnknown State = iota
	StateRunning
	StateStopped
)

type segmentRole int32

const (
	segmentRolePrimary segmentRole = iota
	segmentRoleBackup
)

type segment struct {
	mutex           sync.Mutex   // Mutex lock for concurrency control as needed.
	role            segmentRole  // Role of this bookie in the segment.
	bound           atomic.Int64 // Bound offset in the segment before which the data can be read with lock free.
	znodePath       string       // ZNode path of this segment.
	bookieZNodePath string       // ZNode path of this bookie for this segment.
}

// Bookie represents storage layer server Bookie concept.
type Bookie struct {
	cfg     *Config // Configuration items.
	address string  // IP address of this bookie.

	mutex sync.Mutex // Mutex lock for global metadata.

	segments map[string]*segment // Involved segment name -> segment

	zkConn        *zk.Conn        // Connection to ZooKeeper.
	zkEventCh     <-chan zk.Event // ZooKeeper event channel.
	zkBookiePath  string          // Ephemeral znode name in ZooKeeper.
	zkBookiesLock *zk.Lock        // Lock of /bookies path.

	storageFree int64 // Free storage space size in byte.
	storageUsed int64 // Used storage space size in byte(which is # times of a segment).
	state       State // State of this bookie.

	openSegmentFiles map[string]*os.File

	eventCh chan *Event // Channel for receiving events.

	rpcListener net.Listener // RPC listener.
	rpcClients  struct {
		clients map[string]*rpc.Client // RPC network connections to other bookies. Mapping: address->rpc client
		rwMutex sync.RWMutex           // Write request occupies very small fraction, we use RWLock for parallelism.
	}

	logger *util.Logger // Logger to record messages.
}

// GetState returns the state of this bookie.
func (bk *Bookie) GetState() State {
	return State(atomic.LoadInt32((*int32)(&bk.state)))
}

// StrState returns the bookie state string format.
func StrState(state State) string {
	str := ""
	switch state {
	case StateUnknown:
		str = "Unknown"
	case StateRunning:
		str = "Running"
	case StateStopped:
		str = "Stopped"
	}
	return str
}

// NewBookie generates a new bookie with the configuration cfg.
func NewBookie(cfg *Config) (*Bookie, error) {
	bk := new(Bookie)
	err := error(nil)

	bk.cfg = cfg
	bk.address = fmt.Sprintf("%s:%d", bk.cfg.IPAddress, bk.cfg.Port)

	// initialize the log.
	logFileBaseName := fmt.Sprintf("bookie-%v-%v.log", cfg.Port, os.Getpid())
	logFilePath := filepath.Join(cfg.LogFilePath, logFileBaseName)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return nil, err
	}
	bk.logger = util.NewLogger(logFile, util.DebugLevel)
	bk.logger.Infof("Bookie begins to initilize.")

	// initialize the event handlers.
	bk.eventCh = make(chan *Event, maxEventBackLog)

	bk.segments = make(map[string]*segment)

	bk.recovery()
	// initialize the file system related fields of this bookie.
	err = bk.initFileSystem()
	if err != nil {
		return nil, err
	}
	bk.logger.Infof("Bookie initialize file system successfully.")

	// initialize the zookeeper information of this bookie.
	err = bk.initZK()
	if err != nil {
		return nil, err
	}
	bk.logger.Infof("Bookie initialize zookeeper successfully.")

	// initialize the rpc.
	err = bk.initRPC()
	if err != nil {
		return nil, err
	}
	bk.logger.Infof("Bookie initialize RPC server successfully.")

	// everything goes well, runs the main loop now.
	bk.setState(StateRunning)
	return bk, err
}

func newSegment(role segmentRole, maxOffset int64, bookieZNodePath string, segmentZNodePath string) *segment {
	sg := new(segment)
	sg.role = role
	sg.bound.Store(maxOffset)
	sg.znodePath = segmentZNodePath
	sg.bookieZNodePath = bookieZNodePath
	return sg
}

func (bk *Bookie) shutDown() error {
	if bk.GetState() == StateStopped {
		return fmt.Errorf("already stopped")
	}

	bk.setState(StateStopped)

	bk.zkConn.Close()

	err := bk.closeRPC()
	if err != nil {
		return err
	}
	return nil
}

// ShutDown cleans the state of bookie.
func (bk *Bookie) ShutDown() {
	bk.logger.Infof("Bookie [%v] shutting down...", os.Getpid())
	closeErr := bk.shutDown()
	if closeErr != nil {
		bk.logger.Errorf("Bookie [%v] shut down with error : %v", os.Getpid(), closeErr)
	} else {
		bk.logger.Infof("Bookie [%v] shut down successfully", os.Getpid())
	}

}

func (bk *Bookie) Run() {
	defer func() {
		if err := recover(); err != nil {
			bk.logger.Errorf("Bookie panic, %v", err)
		}
		bk.ShutDown()
	}()
	bk.logger.Infof("Bookie [%v] starts running in main loop...", os.Getpid())
	bk.logger.Infof("With config:\n %+v", bk.cfg.toJsonString())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	for bk.GetState() == StateRunning {
		select {
		case event := <-bk.eventCh:
			bk.handleEvent(event)
		case <-time.After(bk.cfg.CronTimer):
			event := &Event{
				Description: "Cron",
				eType:       eventCron,
				args:        nil,
			}
			bk.handleEvent(event)
		case <-sigCh:
			bk.ShutDown()
			return
		}
	}
}

func (bk *Bookie) recovery() {
	currentTime := time.Now()
	err := filepath.WalkDir(bk.cfg.StorageDirectoryPath, func(path string, d fs.DirEntry, err error) error {
		info, err := d.Info()
		if err != nil {
			return err
		}
		elapsedTime := currentTime.Sub(info.ModTime())
		if elapsedTime < bk.cfg.PermissibleDowntime {
			// This segment is reserved to be request data from zk.
			bk.segments[filepath.Base(path)] = nil
			bk.storageUsed += bk.cfg.SegmentMaxSize
			bk.storageFree -= bk.cfg.SegmentMaxSize
		}
		return nil
	})
	if err != nil {
		bk.logger.Errorf("recovery: %v", err)
	}
}

// setState sets the state of this bookie atomically.
func (bk *Bookie) setState(state State) {
	bk.logger.Infof("Bookie state is set to %v", StrState(state))
	atomic.StoreInt32((*int32)(&bk.state), int32(state))
}
