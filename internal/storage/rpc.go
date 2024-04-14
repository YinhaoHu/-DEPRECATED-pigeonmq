package storage

import (
	"errors"
	"fmt"
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
	ErrSegmentLag = errors.New("bookie: backup segment did not follow up the primary segment")
)

type CreatePrimarySegmentArgs struct {
	SegmentName string        // The name of the segment to be created.
	Timeout     time.Duration // Specify the timeout for the bookie to communicate with peers.
}

// CreatePrimarySegment creates a primary segment and this bookie is the leader of this segment initially.
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
		return fmt.Errorf("bookie segment %v already exists", args.SegmentName)
	}

	// Acquire ZooKeeper lock '/bookies' for selecting followers.
	err = bk.zkBookiesLock.Lock()
	if err != nil {
		log.Printf("zkBookies lock failed: %v", err)
		return err
	}
	defer bk.zkBookiesLock.Unlock()

	// get bookies
	peers, err := bk.getPeersOnZK()
	if err != nil {
		return err
	}

	// select followers.
	segmentBookies := make(map[string]*segmentBookieZNode)
	segmentBookies["me"] = &segmentBookieZNode{address: bk.address}
	followers, err := bk.selectSegmentFollower(peers, segmentBookies, bk.cfg.ReplicateNumber-1, bk.cfg.SegmentMaxSize)

	// create segment on fs and zk.
	fsErr := bk.createSegmentOnFS(args.SegmentName)
	if fsErr != nil {
		return fsErr
	}
	segmentZNodePath, bookieZNodePath, zkErr := bk.createSegmentOnZK(args.SegmentName)
	if zkErr != nil {
		return zkErr
	}
	zkErr = bk.updateBookieOnZK(bk.getStateOnZK())
	if zkErr != nil {
		return zkErr
	}
	bk.segments[args.SegmentName] = newSegment(segmentRolePrimary, segmentHeaderSize, bookieZNodePath, segmentZNodePath)

	// notify some peers that they are the followers now.
	createBackupSegmentArgs := &CreateBackupSegmentArgs{args.SegmentName}
	wg := sync.WaitGroup{}
	for _, follower := range followers {
		wg.Add(1)
		go func() {
			rpcErr := bk.sendRPC(follower, "Bookie.CreateBackupSegment", &createBackupSegmentArgs, nil, args.Timeout)
			if rpcErr != nil {
				bk.logger.Errorf("CreateBackupSegment RPC failed: %v", rpcErr)
			}
			wg.Done()
		}()
	}
	wg.Wait()

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
		return fmt.Errorf("bookie segment %v already exists", args.SegmentName)
	}

	// create segment on local.
	err = bk.createSegmentOnFS(args.SegmentName)
	if err != nil {
		return err
	}

	// create bookie znode in this segment.
	segmentZNodePath := filepath.Join(filepath.Join(zkSegmentsPath, args.SegmentName))
	bookieZNodePath, err := bk.createSegmentBookieOnZK(bk.address, segmentZNodePath)
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
func (bk *Bookie) ReadSegment(args *ReadSegmentArgs, reply *ReadSegmentReply) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC ReadSegment, args=%v", *args)
	defer func() {
		bk.logger.Infof("Bookie handled RPC ReadSegment, error=%v", err)
	}()
	// Read without lock.
	maxSize := min(args.MaxSize, bk.segments[args.SegmentName].bound.Load()-args.BeginPos)
	reply.Data, reply.NBytes, err = bk.readSegmentOnFS(args.SegmentName, args.BeginPos, maxSize)

	return err
}

type AppendPrimarySegmentArgs struct {
	SegmentName string
	Data        []byte
	Timeout     time.Duration
}

type AppendPrimarySegmentReply struct {
	BeginPos int64
}

// AppendPrimarySegment appends data in the segment specified by segmentID.
func (bk *Bookie) AppendPrimarySegment(args *AppendPrimarySegmentArgs, reply *AppendPrimarySegmentReply) error {
	// Prepare response.
	err := error(nil)
	bk.logger.Infof("Bookie received RPC AppendPrimarySegment, args=%v", *args)
	defer func() { bk.logger.Infof("Bookie handled RPC AppendPrimarySegment, error=%v", err) }()
	segmentPath := filepath.Join(filepath.Join(zkSegmentsPath, args.SegmentName))

	// Acquire mutex for this segment.
	bk.segments[args.SegmentName].mutex.Lock()
	defer bk.segments[args.SegmentName].mutex.Unlock()

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
	if atomic.LoadInt32(&successCount) < int32(bk.cfg.MinimumReplicaNumber) {
		err = errors.New("bookie failed to append primary segment because the followers cannot response on time")
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

func (bk *Bookie) AppendBackupSegment(args *AppendBackupSegmentArgs, _ *struct{}) error {
	err := error(nil)
	bk.logger.Infof("Bookie received RPC AppendBackupSegment, segmentName=%v, dataLen=%v",
		args.SegmentName, len(args.Data))
	go func() { bk.logger.Infof("Bookie hanlded AppendBackupSegment, error=%v", err) }()

	// Acquire mutex for this segment.
	bk.segments[args.SegmentName].mutex.Lock()
	defer bk.segments[args.SegmentName].mutex.Unlock()

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

// initRPC initializes the RPC settings.
func (bk *Bookie) initRPC() error {
	err := error(nil)
	// Register rpc and open the rpc server.
	err = rpc.Register(bk)
	if err != nil {
		panic(err)
	}
	rpc.HandleHTTP()
	address := bk.address
	bk.rpcListener, err = net.Listen("tcp", address)
	if err != nil {
		bk.logger.Errorf("initRPC err %v", err)
	}

	// The connections to many bookie RPC servers.
	bk.rpcClients.clients = make(map[string]*rpc.Client)

	// Serve the rpc.
	go http.Serve(bk.rpcListener, nil)
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
	bk.rpcClients.rwMutex.RLock()
	client, exists := bk.rpcClients.clients[address]
	// If the connection to the specific RPC server does not open, open it.
	if !exists {
		bk.rpcClients.rwMutex.RUnlock()
		bk.rpcClients.rwMutex.Lock()
		client, exists = bk.rpcClients.clients[address]
		// Checks whether the other goroutine has created the client.
		if !exists {
			newClient, err := rpc.DialHTTP("tcp", address)
			if err != nil {
				bk.rpcClients.rwMutex.Unlock()
				bk.logger.Errorf("sendRPC rpc.DialHTTP(tcp,%v) err %v", address, err)
				return err
			}
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
	return err
}
