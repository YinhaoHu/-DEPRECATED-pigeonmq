package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"path/filepath"
	"pigeonmq/internal/util"
	"time"
	"unsafe"
)

const (
	zkBookiesPath           = "/bookies"
	zkBookiesLockPath       = "/locks/bookies"
	zkBookieZNodeNamePrefix = "bookie"

	zkSegmentsPath             = "/segments"
	zkSegmentBookieNamePrefix  = "bookie"
	zkSegmentMetadataZNodeName = "metadata"
)

// BookieZKLogger represents the type for zk package to print log with.
type BookieZKLogger struct {
	bookieLogger *util.Logger
}

func (l *BookieZKLogger) Printf(str string, v ...interface{}) {
	zkMsg := fmt.Sprintf(str, v...)
	fullMsg := fmt.Sprintf("ZooKeeper %v", zkMsg)
	l.bookieLogger.Infof(fullMsg)
}

// initZK will set the initial information in zookeeper directory "/pigeonmq/bookies"
func (bk *Bookie) initZK() error {
	err := error(nil)

	// connect to zk with options.
	zkOptionSetLogger := func(conn *zk.Conn) {
		zkLogger := &BookieZKLogger{bookieLogger: bk.logger}
		conn.SetLogger(zkLogger)
	}
	bk.zkConn, bk.zkEventCh, err = zk.Connect(bk.cfg.ZooKeeperClusterAddresses, bk.cfg.ZooKeeperConnectionTimeout,
		zkOptionSetLogger)

	// Check existence of essential paths.
	paths := []string{zkBookiesPath, zkSegmentsPath}
	for _, path := range paths {
		exists, _, existsErr := bk.zkConn.Exists(path)
		if existsErr != nil {
			return fmt.Errorf("initZK: %w", existsErr)
		}
		if !exists {
			err = fmt.Errorf("initZK: path %v does not exist", path)
			return err
		}
	}

	// Create the ZKState.
	bk.zkBookiePath, err = bk.createBookieOnZK()

	// Initialize the zk locks.
	bk.zkBookiesLock = zk.NewLock(bk.zkConn, zkBookiesLockPath, zk.WorldACL(zk.PermAll))

	// Come back to the segments this bookie is responsible for.
	// FIXME: there is a chance that when the leader selected another bookie after PermissibleDowntime,
	//  but this bookie still comes back. This is currently allowed because of the small possibility.
	//  What if all of the bookies in a segment crash? How could leader be elected? (Future TO-DO)
	for segmentName, _ := range bk.segments {
		bk.logger.Infof("initZK: create segment %v hinted by recovery", segmentName)
		segmentZNodePath := filepath.Join(zkSegmentsPath, segmentName)
		bookieZNodePath, recoveryErr := bk.createSegmentBookieOnZK(bk.address, segmentZNodePath,
			int(bk.segments[segmentName].bound.Load()))

		if recoveryErr != nil {
			if errors.Is(recoveryErr, zk.ErrNoNode) {
				bk.logger.Warnf("initZK: create segment %v hinted by recovery, segment znode does not exist. "+
					"All bookies for this segment crahsed?",
					segmentName)
				delete(bk.segments, segmentName)
				continue
			}
			bk.logger.Errorf("initZK: create segment %v hinted by recovery err %v", segmentName, recoveryErr)
			return recoveryErr
		}
		bk.segments[segmentName].bookieZNodePath = bookieZNodePath
		bk.segments[segmentName].znodePath = segmentZNodePath
	}

	return err
}

// createSegmentOnZK creates a segment znode on zk for segment leader.
//
// Note: If err is zk.ErrNodeExists, check whether this happens after a leader crash or follower crash by
// checking whether the bookieZNodePath is empty or not.
func (bk *Bookie) createSegmentOnZK(segmentName string) (segmentZNodePath string, bookieZNodePath string, err error) {
	// Create segment znode.
	segmentZNodePath = filepath.Join(zkSegmentsPath, segmentName)
	_, err = bk.zkConn.Create(segmentZNodePath, nil, 0, zk.WorldACL(zk.PermAll))

	if err != nil {
		if !errors.Is(err, zk.ErrNodeExists) {
			return "", "", fmt.Errorf("createSegmentOnZK:create segment znode %w", err)
		}
		// ErrNodeExists is allowed because if the previous leader created this but error happens.
		// the client resend the createPrimarySegmentRPC request.
		// If the error is leader crash, bookieZNodePath is empty.
		// If the error is follower crash, bookieZNodePath is valid.
		segmentBookies, err2 := bk.getSegmentBookiesOnZK(segmentZNodePath)
		if err2 != nil {
			return "", "", fmt.Errorf("createSegmentOnZK:create segment bookies %w", err2)
		}
		bookieZNodePath = ""
		for znodeName, bookie := range segmentBookies {
			if bookie.address == bk.address {
				bookieZNodePath = filepath.Join(zkSegmentsPath, znodeName)
			}
		}
		return segmentZNodePath, bookieZNodePath, err
	}

	// Create initial leader bookie znode under this segment.
	address := fmt.Sprintf("%v:%v", bk.cfg.IPAddress, bk.cfg.Port)
	bookieZNodePath, err = bk.createSegmentBookieOnZK(address, segmentZNodePath, segmentHeaderSize)
	if err != nil {
		return "", "", fmt.Errorf("createSegmentOnZK:create initial leader bookie znode %w", err)
	}

	// Create metadata znode under this segment.
	metadataZNodePath := filepath.Join(segmentZNodePath, zkSegmentMetadataZNodeName)
	metadataZNode := segmentMetadataZNode{address}
	_, err = bk.zkConn.Create(metadataZNodePath, metadataZNode.toBytes(), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", "", fmt.Errorf("createSegmentOnZK:create metadata znode %w", err)
	}

	return segmentZNodePath, bookieZNodePath, nil
}

// createSegmentBookieOnZK creates a bookie znode under a segment path.
func (bk *Bookie) createSegmentBookieOnZK(address string, segmentZNodePath string, offset int) (bookieZNodePath string, err error) {
	bookieZNodePath = filepath.Join(segmentZNodePath, zkSegmentBookieNamePrefix)
	bookieZNode := segmentBookieZNode{address, offset}

	// Ephemeral flag is used so that a machine crash could be detected,
	// sequence flag is used so that if more than one bookie has the same highest offset,
	// the leader can be elected and made an agreement easily.
	bookieZNodePath, err = bk.zkConn.Create(bookieZNodePath, bookieZNode.toBytes(), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	return bookieZNodePath, err
}

// createBookieOnZK creates a bookie znode on zk for bookie membership management.
func (bk *Bookie) createBookieOnZK() (bookieZNodePath string, err error) {
	bookieZNodePath = filepath.Join(zkBookiesPath, zkBookieZNodeNamePrefix)
	bookieZNode := BookieZKState{fmt.Sprintf("%v:%v", bk.cfg.IPAddress, bk.cfg.Port), bk.storageFree, bk.storageUsed}
	bookieZNodeBytes, err := bookieZNode.ToBytes()
	if err != nil {
		return "", err
	}
	bookieZNodePath, err = bk.zkConn.Create(bookieZNodePath, bookieZNodeBytes, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return bookieZNodePath, err
}

// updateBookieOnZK update the bookie state znode in the directory bookies.
func (bk *Bookie) updateBookieOnZK(state *BookieZKState) error {
	data, err := state.ToBytes()
	if err != nil {
		return err
	}
	_, err = bk.zkConn.Set(bk.zkBookiePath, data, -1)
	return err
}

// getPeersOnZK returns the states of the peers except this bookie's state.
func (bk *Bookie) getPeersOnZK() (peers []*BookieZKState, err error) {
	bookieStateZNodes, _, err := bk.zkConn.Children(zkBookiesPath)
	if err != nil {
		return nil, err
	}
	peers = make([]*BookieZKState, 0)
	for _, znodeName := range bookieStateZNodes {
		if znodeName != bk.zkBookiePath {
			znodePath := filepath.Join(zkBookiesPath, znodeName)
			znodeBytes, _, getErr := bk.zkConn.Get(znodePath)
			if getErr != nil {
				return nil, getErr
			}
			znode, decErr := NewBookieStateFromBytes(znodeBytes)
			if decErr != nil {
				return nil, decErr
			}
			peers = append(peers, znode)
		}
	}
	return peers, nil
}

// watchSegmentPeersBG watches the followers of the segment on background.
func (bk *Bookie) watchSegmentPeersBG(segmentPath string) {
	// Watch the segment children.
	children, _, eventCh, err := bk.zkConn.ChildrenW(segmentPath)
	if err != nil {
		bk.logger.Errorf("watchSegmentPeersBG meet error %v", err)
		return
	}

	// Send to main loop.
	go func() {
		event := <-eventCh
		currentFollowers, _, err2 := bk.zkConn.Children(event.Path)
		if err2 != nil {
			bk.logger.Errorf("watchSegmentPeersBG meet error %v", err2)
			return
		}
		bk.logger.Infof("watchSegmentPeersBG meet event %v", event)
		if event.Type == zk.EventNodeChildrenChanged {
			evt := &Event{
				Description: "follower updates",
				eType:       eventFollowerUpdate,
				args: eventFollowerUpdateArgs{filepath.Base(segmentPath), children,
					currentFollowers},
			}
			if len(currentFollowers) < len(children) {
				// If one follower crashed, we delay the event signal.
				time.Sleep(bk.cfg.PermissibleDowntime)
			}
			bk.eventCh <- evt
			bk.logger.Infof("Bookie send follower update event %v", *evt)
			return
		}
	}()

}

// watchSegmentMetadataBG watches the leader of the segment on background.
func (bk *Bookie) watchSegmentMetadataBG(segmentPath string) {
	metadataPath := filepath.Join(segmentPath, zkSegmentMetadataZNodeName)
	_, _, eventCh, err := bk.zkConn.GetW(metadataPath)
	sendLeaderCrashEvent := func() {
		event := &Event{
			Description: "leader crash",
			eType:       eventLeaderCrash,
			args:        eventLeaderCrashArgs{filepath.Base(segmentPath)},
		}
		bk.eventCh <- event
		bk.logger.Infof("Bookie send leader crash event. event=%v", *event)
	}

	if err != nil {
		// leader has already been crashed.
		if errors.Is(err, zk.ErrNoNode) {
			sendLeaderCrashEvent()
		} else {
			bk.logger.Warnf("watchSegmentMetadataBG meet unexpected zk error. err=%v", err)
			return
		}
	}
	go func() {
		e := <-eventCh
		if e.Type != zk.EventNodeDeleted {
			bk.logger.Warnf("watchSegmentMetadataBG meet unexpected watched zk event,event=%v", e)
			return
		}
		sendLeaderCrashEvent()
	}()
}

// getSegmentBookiesOnZK returns all the bookie znode on the segment including the invoker bookie.
//
// Return value `bookies` is the znodeName->bookieZNode mapping.
func (bk *Bookie) getSegmentBookiesOnZK(segmentPath string) (bookies map[string]*segmentBookieZNode, err error) {
	bookies = make(map[string]*segmentBookieZNode)
	children, _, err := bk.zkConn.Children(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("getSegmentBookiesOnZK(%v): zk.children(%v) %w",
			segmentPath, segmentPath, err)
	}
	for _, child := range children {
		// ignore metadata znode, we just get the bookie znode.
		if child == zkSegmentMetadataZNodeName {
			continue
		}
		bookieSegmentZNodePath := filepath.Join(segmentPath, child)
		znodeBytes, _, getErr := bk.zkConn.Get(bookieSegmentZNodePath)
		if getErr != nil {
			return nil, fmt.Errorf("getSegmentBookiesOnZK(%v): zk.Get(%v) %w",
				segmentPath, bookieSegmentZNodePath, getErr)
		}
		bookies[child] = newSegmentBookieZnode(znodeBytes)
	}
	return bookies, nil
}

// getStateOnZK returns the bookie state on ZK.
func (bk *Bookie) getStateOnZK() *BookieZKState {
	zkState := new(BookieZKState)
	zkState.Address = bk.address
	zkState.StorageUsed = bk.storageUsed
	zkState.StorageFree = bk.storageFree
	return zkState
}

// segmentBookieZNode represents the bookie znode under a segment path.
type segmentBookieZNode struct {
	address string // The IP address and port of this bookie.
	offset  int    // The current committed offset of the bookie in the segment's payload.
}

// toBytes convert the segmentBookieZnode variable to bytes.
func (s *segmentBookieZNode) toBytes() []byte {
	addrBytes := []byte(s.address)
	buf := make([]byte, len(addrBytes)+int(unsafe.Sizeof(s.offset)))
	copy(buf, addrBytes)
	binary.BigEndian.PutUint64(buf[len(addrBytes):], uint64(s.offset))
	return buf
}

// newSegmentBookieZNode convert the bytes to a segmentBookieZNode instance.
func newSegmentBookieZnode(data []byte) *segmentBookieZNode {
	addrLen := len(data) - int(unsafe.Sizeof(0))
	address := string(data[:addrLen])
	offset := binary.BigEndian.Uint32(data[addrLen:])
	return &segmentBookieZNode{
		address: address,
		offset:  int(offset),
	}
}

// segmentMetadataZNode represents the metadata znode under a segment path.
type segmentMetadataZNode struct {
	leaderAddress string // The IP leaderAddress and port of the leader.
}

// toBytes converts the segmentMetadataZNode variable to bytes.
func (s *segmentMetadataZNode) toBytes() []byte {
	addrBytes := []byte(s.leaderAddress)
	buf := make([]byte, len(addrBytes))
	copy(buf, addrBytes)
	return buf
}

// BookieZKState represents the state of a bookie in ZooKeeper.
// This state is useful for service layer to deal with load balance.
type BookieZKState struct {
	Address     string
	StorageFree int // number of bytes free in the storage.
	StorageUsed int // number of used bytes in the storage
}

// ToBytes serializes the BookieZKState struct to bytes using gob.
func (s *BookieZKState) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// NewBookieStateFromBytes deserializes bytes to a BookieZKState variable.
func NewBookieStateFromBytes(data []byte) (*BookieZKState, error) {
	var state BookieZKState
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}
