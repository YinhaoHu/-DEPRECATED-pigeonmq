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
	"strings"
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
	bound           atomic.Int32 // Bound offset in the segment before which the data can be read with lock free.
	znodePath       string       // ZNode path of this segment.
	bookieZNodePath string       // ZNode path of this bookie for this segment.
	state           segmentState // Open or close
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

	storageFree int   // Free storage space size in byte.
	storageUsed int   // Used storage space size in byte(which is # times of a segment).
	state       State // State of this bookie.

	openSegmentFiles map[string]*os.File

	eventCh chan *Event // Channel for receiving events.

	rpcServer   *rpc.Server
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
	logFile, err := os.Create(logFilePath)

	if err != nil {
		return nil, err
	}
	bk.logger = util.NewLogger(logFile, util.DebugLevel)
	bk.logger.Infof("Bookie begins to initilize.")

	// allocate memory for fields
	bk.eventCh = make(chan *Event, maxEventBackLog)
	bk.segments = make(map[string]*segment)
	bk.openSegmentFiles = make(map[string]*os.File)

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

func newSegment(role segmentRole, maxOffset int, bookieZNodePath string, segmentZNodePath string) *segment {
	sg := new(segment)
	sg.role = role
	sg.bound.Store(int32(maxOffset))
	sg.znodePath = segmentZNodePath
	sg.bookieZNodePath = bookieZNodePath
	sg.state = segmentStateOpen
	return sg
}

// RPCPath returns the rpc path of a specific bookie in address.
func RPCPath(address string) string {
	address = strings.Replace(address, ".", "_", -1)
	address = strings.Replace(address, ":", "_", -1)
	return fmt.Sprintf("/_go_rpc_%v", address)
}

// Shutdown cleans the state of bookie.
func (bk *Bookie) Shutdown() {
	bk.logger.Infof("Bookie [%v] shutting down...", os.Getpid())
	closeErr := error(nil)
	if bk.GetState() == StateStopped {
		closeErr = fmt.Errorf("already stopped")
	} else {
		bk.setState(StateStopped)
		bk.zkConn.Close()
		closeErr = bk.closeRPC()
	}
	if closeErr != nil {
		bk.logger.Errorf("Bookie [%v] shut down with error : %v", os.Getpid(), closeErr)
	} else {
		bk.logger.Infof("Bookie [%v] shut down successfully", os.Getpid())
	}
}

// Run runs the bookie routine. Typically, you should invoke this in a go statement.
func (bk *Bookie) Run() {
	defer func() {
		if err := recover(); err != nil {
			bk.logger.Errorf("Bookie panic, %v", err)
		}
		bk.Shutdown()
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
			bk.Shutdown()
			return
		}
	}
}

func (bk *Bookie) recovery() {
	bk.logger.Infof("start recovery process")

	// Check whether the directory path exists.
	if _, err := os.Stat(bk.cfg.StorageDirectoryPath); os.IsNotExist(err) {
		mkdirErr := os.MkdirAll(bk.cfg.StorageDirectoryPath, 0755)
		if mkdirErr != nil {
			bk.logger.Errorf("initFileSystem: %v", mkdirErr)
			panic(mkdirErr)
		}
		bk.logger.Infof("initFileSystem: storage directory path %v does not exists. Automatically created.",
			bk.cfg.StorageDirectoryPath)
	}

	currentTime := time.Now()
	err := filepath.WalkDir(bk.cfg.StorageDirectoryPath, func(path string, d fs.DirEntry, err error) error {
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		elapsedTime := currentTime.Sub(info.ModTime())
		bk.logger.Infof("recovery: meet segment %v. ElapsedTime: %v", filepath.Base(path), elapsedTime)
		if elapsedTime < bk.cfg.PermissibleDowntime {
			// This segment is reserved to be request data from zk.
			hdr := bk.getSegmentHeaderOnFS(filepath.Base(path))
			bk.logger.Infof("recovery: begin to be responsible for segment %v", filepath.Base(path))
			bk.segments[filepath.Base(path)] = newSegment(segmentRoleBackup, hdr.payloadSize+segmentHeaderSize, "", "")
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
