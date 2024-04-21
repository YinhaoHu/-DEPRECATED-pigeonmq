package storage

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrLackFollowers   = errors.New("bookie: no enough followers response")
	ErrSegmentLag      = errors.New("bookie: backup segment did not follow up the primary segment")
	ErrSegmentExists   = errors.New("bookie: segment already exists")
	ErrSegmentNotFound = errors.New("bookie: segment not found")
	ErrSegmentBadRead  = errors.New("bookie: reading an open segment from a follower")
	ErrSegmentClosed   = errors.New("bookie: segment already closed")
)

// CreatePrimarySegmentArgs represents the argument type of CreatePrimarySegment RPC.
type CreatePrimarySegmentArgs struct {
	SegmentName string        // The name of the segment to be created. Typically, this is "topic/partition".
	Timeout     time.Duration // Specify the timeout for the bookie to communicate with peers.
}

// CreatePrimarySegment creates a primary segment and this bookie is the leader of this segment initially.
//
// Guarantee: Returning without any error indicates the segment is replicated on ReplicaNumber bookies successfully.
func (bk *Bookie) CreatePrimarySegment(args *CreatePrimarySegmentArgs, _ *struct{}) error {
	err := error(nil)
	defer func() {
		bk.logger.Infof("Bookie handled RPC CreatePrimarySegment, error=%v", err)
	}()

	// Acquire lock for segments field change.
	bk.mutex.Lock()
	defer bk.mutex.Unlock()

	// Checks whether this segment existed.
	_, exist := bk.segments[args.SegmentName]
	if exist {
		return ErrSegmentExists
	}

	// Acquire ZooKeeper lock '/bookies' for selecting followers.
	err = bk.zkBookiesLock.Lock()
	if err != nil {
		log.Printf("zkBookies lock failed: %v", err)
		return err
	}
	defer func(zkBookiesLock *zk.Lock) {
		_ = zkBookiesLock.Unlock()
	}(bk.zkBookiesLock)

	// get bookies
	peers, err := bk.getPeersOnZK()
	if err != nil {
		return err
	}
	bk.logger.Infof("Bookie got peers: %+v", peers)
	for _, peer := range peers {
		bk.logger.Infof("Bookie got peer: %+v", peer)
	}

	// select followers.
	segmentBookies := make(map[string]*segmentBookieZNode)
	segmentBookies["me"] = &segmentBookieZNode{address: bk.address}
	followers, err := bk.selectSegmentFollower(peers, segmentBookies, bk.cfg.ReplicateNumber-1, bk.cfg.SegmentMaxSize)
	if err != nil {
		return err
	}

	// create segment on fs and zk.
	fsErr := bk.createSegmentOnFS(args.SegmentName)
	if fsErr != nil {
		return fsErr
	}
	segmentZNodePath, bookieZNodePath, zkErr := bk.createSegmentOnZK(args.SegmentName)
	if zkErr != nil {
		if !errors.Is(zkErr, zk.ErrNodeExists) {
			return zkErr
		}
		if bookieZNodePath == "" {
			bookieZNodePath, zkErr = bk.createSegmentBookieOnZK(bk.address, segmentZNodePath, segmentHeaderSize)
			if zkErr != nil {
				return zkErr
			}
		}
	}
	zkErr = bk.updateBookieOnZK(bk.getStateOnZK())
	if zkErr != nil {
		return zkErr
	}

	// notify some peers that they are the followers now.
	createBackupSegmentArgs := &CreateBackupSegmentArgs{args.SegmentName}
	successCount := atomic.Int32{}
	successCount.Store(1)
	wg := sync.WaitGroup{}
	for _, follower := range followers {
		wg.Add(1)
		go func() {
			rpcErr := bk.sendRPC(follower, "Bookie.CreateBackupSegment", &createBackupSegmentArgs, nil, args.Timeout)
			if errors.Is(rpcErr, ErrSegmentExists) {
				bk.logger.Warnf("Bookie got backup segment already exists: %v", rpcErr)
				rpcErr = nil
			}
			if rpcErr != nil {
				bk.logger.Errorf("CreateBackupSegment RPC failed: %v", rpcErr)
			} else {
				successCount.Add(1)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if successCount.Load() != bk.cfg.ReplicateNumber {
		err = ErrLackFollowers
	}

	// Everything works well, we make the state visible.
	if err == nil {
		bk.segments[args.SegmentName] = newSegment(segmentRolePrimary, segmentHeaderSize, bookieZNodePath, segmentZNodePath)
		bk.watchSegmentPeersBG(segmentZNodePath)
		bk.logger.Infof("Bookie created a new segment: %v", args.SegmentName)
	}

	return err
}

type CreateBackupSegmentArgs struct {
	SegmentName string
}

// CreateBackupSegment creates a backup segment and this bookie is the follower of this segment initially.
func (bk *Bookie) CreateBackupSegment(args *CreateBackupSegmentArgs, _ *struct{}) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC CreateBackupSegment, args=%v", args)
	defer func() {
		bk.logger.Infof("Bookie handled RPC CreateBackupSegment: error=%v", err)
	}()

	// Acquire lock for segments field change.
	bk.mutex.Lock()
	defer bk.mutex.Unlock()

	// Checks whether this segment existed.
	_, exist := bk.segments[args.SegmentName]
	if exist {
		return ErrSegmentExists
	}

	// create segment on local.
	err = bk.createSegmentOnFS(args.SegmentName)
	if err != nil {
		return err
	}

	// create bookie znode in this segment.
	segmentZNodePath := filepath.Join(filepath.Join(zkSegmentsPath, args.SegmentName))
	bookieZNodePath, err := bk.createSegmentBookieOnZK(bk.address, segmentZNodePath, segmentHeaderSize)
	if err != nil {
		return err
	}
	bk.segments[args.SegmentName] = newSegment(segmentRoleBackup, segmentHeaderSize, bookieZNodePath, segmentZNodePath)

	// update the resource usage of this bookie.
	err = bk.updateBookieOnZK(bk.getStateOnZK())
	if err != nil {
		return err
	}

	// monitor leader.
	bk.watchSegmentMetadataBG(segmentZNodePath)

	return err
}

type ReadSegmentArgs struct {
	SegmentName string
	BeginPos    int64
	MaxSize     int64
}

type ReadSegmentReply struct {
	Data   []byte
	NBytes int64 // Number of actual read bytes.
}

// ReadSegment reads maxSize bytes from the beginPos in the segment specified by segmentID.
//
// Guarantee: Satisfy the guarantee mentioned by `docs/specifications/storage-layer.md-overview`.
// Duplication read is possible.
func (bk *Bookie) ReadSegment(args *ReadSegmentArgs, reply *ReadSegmentReply) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC ReadSegment, args=%v", *args)
	defer func() {
		bk.logger.Infof("Bookie handled RPC ReadSegment, error=%v", err)
	}()

	// Reading from an open backup segment is forbidden.
	seg, exist := bk.segments[args.SegmentName]
	if !exist {
		return ErrSegmentNotFound
	}
	seg.mutex.Lock()
	if seg.state != segmentStateClose && seg.role == segmentRoleBackup {
		seg.mutex.Unlock()
		return ErrSegmentBadRead
	}
	seg.mutex.Unlock()

	// Read without lock, avoiding long time block when issuing IO.
	maxSize := min(args.MaxSize, seg.bound.Load()-args.BeginPos)
	reply.Data, reply.NBytes, err = bk.readSegmentOnFS(args.SegmentName, args.BeginPos, maxSize)

	// TODO(Hoo@Future): Observe that if `Config.MinimumReplicaNumber` is greater than half of
	//  `Config.ReplicaNumber`, and the segment offset, let's call it `off`, is included in
	//  at least `Config.MinimumReplicaNumber` bookies, it should be regarded as safe to read
	//  [0, `off]. That is an optimization to improve reduce the overhead of leader. Consider
	//  to introduce a field named committed_offset.

	return err
}

type AppendPrimarySegmentArgs struct {
	SegmentName string
	Data        []byte
	Timeout     time.Duration
}

type AppendPrimarySegmentReply struct {
	BeginPos int64 // The begin-pos of the newly added data entry.
}

// AppendPrimarySegment appends data in the segment specified by SegmentName.
//
// Guarantee: Returning without error indicates that the data has been successfully appended to
// Configs.MinimumReplicaNumber bookies. The data appended to the current leader will be discarded
// or duplicated in case of some failures.
func (bk *Bookie) AppendPrimarySegment(args *AppendPrimarySegmentArgs, reply *AppendPrimarySegmentReply) error {
	// Prepare response.
	err := error(nil)
	bk.logger.Infof("Bookie received RPC AppendPrimarySegment, segment=%v, dataLen=%v, timeout=%v",
		args.SegmentName, len(args.Data), args.Timeout)
	defer func() { bk.logger.Infof("Bookie handled RPC AppendPrimarySegment, error=%v", err) }()
	segmentPath := filepath.Join(filepath.Join(zkSegmentsPath, args.SegmentName))

	// Acquire mutex for this segment and check the existence and state of it.
	seg, exist := bk.segments[args.SegmentName]
	if !exist {
		return ErrSegmentNotFound
	}
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if seg.state == segmentStateClose {
		return ErrSegmentClosed
	}

	// Get the other bookies responsible for this segment.
	bookies, err := bk.getSegmentBookiesOnZK(segmentPath)
	if err != nil {
		return err
	}
	myZNodeName := filepath.Base(bk.segments[args.SegmentName].bookieZNodePath)
	myBookieZNode := bookies[myZNodeName]
	delete(bookies, myZNodeName)

	// Wait for MinimumReplicaNumber-1 followers to response with timeout.
	wg := sync.WaitGroup{}
	successCount := int32(1)
	for _, bookie := range bookies {
		wg.Add(1)
		go func(bookie *segmentBookieZNode) {
			data, _, _ := bk.readSegmentOnFS(args.SegmentName, bookie.offset, bk.cfg.SegmentMaxSize)
			data = append(data, args.Data...)
			appendBackupSegmentArgs := &AppendBackupSegmentArgs{args.SegmentName, data, bookie.offset}

			rpcErr := bk.sendRPC(bookie.address, "Bookie.AppendBackupSegment", &appendBackupSegmentArgs, nil, args.Timeout)
			if rpcErr != nil {
				bk.logger.Errorf("AppendBackupSegment RPC failed: %v,follower %v", rpcErr, bookie.address)
			} else {
				atomic.AddInt32(&successCount, 1)
			}
			wg.Done()
		}(bookie)
	}
	wg.Wait()

	// Check whether there are enough followers.
	if atomic.LoadInt32(&successCount) < bk.cfg.MinimumReplicaNumber {
		err = ErrLackFollowers
		return err
	}

	// Update fs state.
	reply.BeginPos, err = bk.appendSegmentOnFS(args.SegmentName, args.Data)
	if err != nil {
		return err
	}

	// Update zk state.
	err = bk.updateBookieOnZK(bk.getStateOnZK())
	if err != nil {
		return err
	}

	myBookieZNode.offset += int64(len(args.Data))
	_, err = bk.zkConn.Set(bk.segments[args.SegmentName].znodePath, myBookieZNode.toBytes(), -1)
	if err != nil {
		return err
	}

	// Update bound.
	bk.segments[args.SegmentName].bound.Add(int64(len(args.Data)))

	return nil
}

type AppendBackupSegmentArgs struct {
	SegmentName string
	Data        []byte
	BeginPos    int64
}

// AppendBackupSegment appends data in the segment specified by segmentID. This RPC is not public, which means
// this RPC should be only invoked by segment leader.
func (bk *Bookie) AppendBackupSegment(args *AppendBackupSegmentArgs, _ *struct{}) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC AppendBackupSegment, segmentName=%v, dataLen=%v",
		args.SegmentName, len(args.Data))
	defer func() { bk.logger.Infof("Bookie hanlded AppendBackupSegment, error=%v", err) }()

	// Acquire mutex for this segment. Check the existence and state of it.
	seg, exist := bk.segments[args.SegmentName]
	if !exist {
		return ErrSegmentNotFound
	}
	seg.mutex.Lock()
	defer seg.mutex.Unlock()
	if seg.state == segmentStateClose {
		return ErrSegmentClosed
	}

	// In case some before append messages lost.
	if bk.segments[args.SegmentName].bound.Load() < args.BeginPos {
		return ErrSegmentLag
	}

	// Update fs state.
	err = bk.appendAtSegmentOnFS(args.SegmentName, args.Data, args.BeginPos, nil)
	if err != nil {
		return err
	}

	// Update zk state.
	bookieOnSegmentZNodePath := bk.segments[args.SegmentName].bookieZNodePath
	myBookieZNodeBytes, _, err := bk.zkConn.Get(bookieOnSegmentZNodePath)
	if err != nil {
		return err
	}
	myBookieZNode := newSegmentBookieZnode(myBookieZNodeBytes)
	err = bk.updateBookieOnZK(bk.getStateOnZK())
	if err != nil {
		return err
	}

	myBookieZNode.offset += int64(len(args.Data))
	_, err = bk.zkConn.Set(bookieOnSegmentZNodePath, myBookieZNode.toBytes(), -1)
	if err != nil {
		return err
	}

	// Update bound.
	bk.segments[args.SegmentName].bound.Add(int64(len(args.Data)))

	return nil
}

type ClosePrimarySegmentArgs struct {
	SegmentName string
	Timeout     time.Duration
}

// ClosePrimarySegment closes the segment specified by SegmentName.
//
// Guarantee: Returning without error indicates that the data has been successfully closed in
// Config.MinimumReplicaNumber bookies.
func (bk *Bookie) ClosePrimarySegment(args *ClosePrimarySegmentArgs, _ *struct{}) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC ClosePrimarySegment, segmentName=%v",
		args.SegmentName)
	defer func() {
		bk.logger.Infof("Bookie hanlded ClosePrimarySegment, error=%v", err)
	}()

	seg, exists := bk.segments[args.SegmentName]
	if !exists {
		return ErrSegmentNotFound
	}

	// Update the state. It's safe to make state visible here.
	seg.mutex.Lock()
	err = bk.closeSegmentOnFS(args.SegmentName)
	if err != nil {
		bk.logger.Errorf("bookie met fs error when close segment %v: %v", args.SegmentName, err)
		seg.mutex.Unlock()
		return err
	}
	seg.state = segmentStateClose
	seg.mutex.Unlock()

	// Get followers.
	segmentPath := filepath.Join(filepath.Join(zkSegmentsPath, args.SegmentName))
	segmentBookies, err := bk.getSegmentBookiesOnZK(segmentPath)
	if err != nil {
		bk.logger.Errorf("Bookie failed to close primary segment %v, error: %v", args.SegmentName, err)
		return err
	}

	// Now, we tell the followers to close the segment.
	wg := sync.WaitGroup{}
	closeBackupSegmentArgs := &CloseBackupSegmentArgs{args.SegmentName}
	successCount := atomic.Int32{}
	successCount.Store(1)
	for _, bookie := range segmentBookies {
		if bookie.address != bk.address {
			wg.Add(1)
			go func() {
				rpcErr := bk.sendRPC(bookie.address, "Bookie.CloseBackupSegment", closeBackupSegmentArgs, nil, args.Timeout)
				if errors.Is(rpcErr, ErrSegmentClosed) {
					bk.logger.Warnf("ClosePrimarySegment: follower %v segment %v was closed already.",
						bookie.address, args.SegmentName)
					rpcErr = nil
				}
				if rpcErr != nil {
					bk.logger.Errorf("ClosePrimarySegment: CloseBackupSegmentRPC error: %v, to %v", err, bookie.address)
				} else {
					successCount.Add(1)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()

	if successCount.Load() < bk.cfg.MinimumReplicaNumber {
		bk.logger.Errorf("ClosePrimarySegment: bookie failed to close primary segment because of no enough " +
			"followers response on time.")
		err = ErrLackFollowers
	}

	return nil
}

type CloseBackupSegmentArgs struct {
	SegmentName string
}

func (bk *Bookie) CloseBackupSegment(args *CloseBackupSegmentArgs, _ *struct{}) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC CloseBackupSegment, segmentName=%v", args.SegmentName)
	defer func() { bk.logger.Infof("Bookie hanlded CloseBackupSegment, error=%v", err) }()

	seg, exists := bk.segments[args.SegmentName]
	if !exists {
		return ErrSegmentNotFound
	}
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	err = bk.closeSegmentOnFS(args.SegmentName)
	if err != nil {
		bk.logger.Errorf("Bookie failed to close backup segment %v, error: %v", args.SegmentName, err)
		return err
	}
	seg.state = segmentStateClose

	return err
}

// initRPC initializes the RPC settings.
func (bk *Bookie) initRPC() error {
	err := error(nil)

	// Register rpc and open the rpc server.
	bk.rpcServer = rpc.NewServer()
	err = bk.rpcServer.Register(bk)
	if err != nil {
		panic(err)
	}
	rpcPath := RPCPath(bk.address)
	debugPath := fmt.Sprintf("/debug/gorpc_%v", bk.address)

	bk.logger.Infof("RPC is handled in path: %v and debug path: %v", rpcPath, debugPath)
	bk.rpcServer.HandleHTTP(rpcPath, debugPath)

	address := bk.address
	bk.rpcListener, err = net.Listen("tcp", address)
	if err != nil {
		bk.logger.Errorf("initRPC err %v", err)
	}

	// The connections to many bookie RPC servers.
	bk.rpcClients.clients = make(map[string]*rpc.Client)

	// Serve the rpc.
	go func() {
		_ = http.Serve(bk.rpcListener, nil)
	}()

	return err
}

// closeRPC cleans the RPC-related state.
func (bk *Bookie) closeRPC() error {
	err := error(nil)

	// Close the RPC connections.
	bk.rpcClients.rwMutex.Lock()
	for _, client := range bk.rpcClients.clients {
		if closeErr := client.Close(); closeErr != nil {
			errors.Join(err, closeErr)
		}
	}
	bk.rpcClients.rwMutex.Unlock()

	// Close the RPC server.
	if closeErr := bk.rpcListener.Close(); closeErr != nil {
		errors.Join(err, closeErr)
	}
	return err
}

// sendRPC is the wrapper for sending RPC with/without timeout.
func (bk *Bookie) sendRPC(address string, method string, args interface{}, reply interface{}, timeout time.Duration) error {
	reconnected := false
	bk.rpcClients.rwMutex.RLock()
	client, exists := bk.rpcClients.clients[address]
start:
	// If the connection to the specific RPC server does not open, open it.
	if !exists {
		bk.rpcClients.rwMutex.RUnlock()
		bk.rpcClients.rwMutex.Lock()
		client, exists = bk.rpcClients.clients[address]
		// Checks whether the other goroutine has created the client.
		if !exists || reconnected {
			bk.logger.Infof("sendRPC: begin to connect to %v", address)
			newClient, err := rpc.DialHTTPPath("tcp", address, RPCPath(address))
			if err != nil {
				bk.rpcClients.rwMutex.Unlock()
				bk.logger.Errorf("sendRPC: rpc.DialHTTP(tcp,%v) err %v", address, err)
				return err
			}
			bk.logger.Infof("sendRPC: connected to %v", address)
			bk.rpcClients.clients[address] = newClient
		}
		client = bk.rpcClients.clients[address]
		bk.rpcClients.rwMutex.Unlock()
	} else {
		bk.rpcClients.rwMutex.RUnlock()
	}
	err := error(nil)
	// Async RPC or sync.
	if timeout > 0 {
		doneCh := make(chan *rpc.Call, 1)
		call := client.Go(method, args, reply, doneCh)
		select {
		case <-time.After(timeout):
			err = errors.Join(errors.New("timeout"), call.Error)
		case <-doneCh:
			err = call.Error
		}
	} else {
		err = client.Call(method, args, reply)
	}

	// Bookie on address is shutdown before. We try to reconnect to it.
	if errors.Is(err, rpc.ErrShutdown) && !reconnected {
		bk.logger.Infof("sendRPC: begin to reconnect %v", address)
		exists = false
		reconnected = true
		bk.rpcClients.rwMutex.RLock()
		goto start
	}
	return err
}
