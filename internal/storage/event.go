package storage

import (
	"github.com/go-zookeeper/zk"
	"net/rpc"
	"path/filepath"
	"time"
)

type eventType int32

const (
	maxEventBackLog = 1024 // The max number of events can be buffered.

	eventUnknown        eventType = 0
	eventLeaderCrash    eventType = 1
	eventFollowerUpdate eventType = 2
	eventCron           eventType = 3
)

// Event represents an event structure.
type Event struct {
	Description string    // additional description about this event.
	eType       eventType // type of event.
	args        any       // args of this event.
}

// handleEvent handles a new event.
func (bk *Bookie) handleEvent(event *Event) {
	bk.logger.Infof("handle event %+v", *event)
	switch event.eType {
	case eventFollowerUpdate:
		args := event.args.(eventFollowerUpdateArgs)
		bk.handleEventFollowerUpdate(&args)
	case eventLeaderCrash:
		args := event.args.(eventLeaderCrashArgs)
		bk.handleEventLeaderCrash(&args)
	case eventCron:
		bk.handleCron()
	case eventUnknown:
		bk.logger.Errorf("handleEvent meets unknown event:%v ", *event)
	}
}

type eventFollowerUpdateArgs struct {
	segmentName        string
	beforeNumFollowers int
}

// handleEventFollowerUpdate acts for a follower coming or leaving the segment bookie group
// which is leaded by this bookie.
func (bk *Bookie) handleEventFollowerUpdate(args *eventFollowerUpdateArgs) {
	// Give the failed bookie some time to come back as needed.
	time.Sleep(bk.cfg.PermissibleDowntime)

	// Get enough data for selecting a backup follower.
	segmentPath := filepath.Join(zkSegmentsPath, args.segmentName)
	segmentBookies, err := bk.getSegmentBookiesOnZK(segmentPath)
	if err != nil {
		bk.logger.Errorf("handleEventFollowerUpdate, error %v", err)
		return
	}

	if args.beforeNumFollowers < len(segmentBookies) {
		// We do nothing when a follower joins.
		bk.logger.Infof("handleEventFollowerUpdate met a new follower comes into the bookie ensemble.")
		return
	}

	peerStates, err := bk.getPeersOnZK()
	if err != nil {
		bk.logger.Errorf("handleEventFollowerUpdate, error %v", err)
		return
	}

	// Select follower now.
	follower, err := bk.selectSegmentFollower(peerStates, segmentBookies, 1, bk.cfg.SegmentMaxSize)
	if len(follower) == 0 {
		bk.logger.Warnf("handleEventFollowerUpdate cannot find enough bookies as followers")
		return
	}
	if err != nil {
		bk.logger.Errorf("handleEventFollowerUpdate, error %v", err.Error())
		return
	}
	bk.logger.Infof("handleEventFollowerUpdate followers selected as %v", follower)

	// Notify the follower.
	createBackupSegmentArgs := &CreateBackupSegmentArgs{SegmentName: args.segmentName}
	client, rpcErr := rpc.DialHTTP("tcp", follower[0])
	if rpcErr != nil {
		bk.logger.Errorf("handleEventFollowerUpdate rpc dial failed, error %v", rpcErr.Error())
	} else {
		doneCh := make(chan *rpc.Call, 1)
		call := client.Go("Bookie.CreateBackupSegment", &createBackupSegmentArgs, nil, doneCh)
		select {
		case <-time.After(bk.cfg.DefaultPeerCommunicationTimeout):
			bk.logger.Warnf("handleEventFollowerUpdate: time out in sending CreateBackupSegment to follower %v, call error %v",
				follower[0], call.Error)
		case doneCall := <-doneCh:
			err = doneCall.Error
			if err != nil {
				bk.logger.Errorf("Bookie.CreatePrimarySegment: error in calling CreateBackupSegment, err=%v,follower=%v", err, follower)
			}
		}
	}

	// Watch this segment again.
	bk.watchSegmentPeersBG(segmentPath)
}

type eventLeaderCrashArgs struct {
	segmentName string
}

// handleEventLeaderCrash acts when a leader crash with a predetermined leader election algorithm
// which is safe when multiple followers elect at the same time.
func (bk *Bookie) handleEventLeaderCrash(args *eventLeaderCrashArgs) {
	// Get the responsible bookies for this segment.
	segmentZNodePath := filepath.Join(zkSegmentsPath, args.segmentName)
	segmentBookies, zkErr := bk.getSegmentBookiesOnZK(segmentZNodePath)
	if zkErr != nil {
		bk.logger.Errorf("Failed to get the responsible bookies for this segment %v, err %v",
			segmentZNodePath, zkErr)
		return
	}

	// Choose the leader with predetermined algorithm.
	leaderAddress, err := bk.electSegmentLeader(segmentBookies)
	if err != nil {
		bk.logger.Errorf("Failed to elect leader segmentBookies %v, err %v", segmentBookies, err)
		return
	}
	bk.logger.Infof("elected leader %v", leaderAddress)

	// This bookie's fate.
	if leaderAddress == bk.address {
		// This bookie converts to leader.
		bk.segments[args.segmentName].role = segmentRolePrimary
		metadata := segmentMetadataZNode{bk.address}
		_, zkErr = bk.zkConn.Create(filepath.Join(segmentZNodePath, zkSegmentMetadataZNodeName), metadata.toBytes(),
			zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if zkErr != nil {
			bk.logger.Errorf("Failed to set segment metadata, %v", zkErr.Error())
			return
		}
		bk.watchSegmentPeersBG(segmentZNodePath)
		// leader crashes means that after this bookie becomes the leader, there misses one follower.
		event := &Event{
			Description: "follower crash",
			eType:       eventFollowerUpdate,
			args:        eventFollowerUpdateArgs{filepath.Base(segmentZNodePath), len(segmentBookies)},
		}
		bk.eventCh <- event
		bk.logger.Infof("Bookie send follower crash event.")
	} else {
		// This bookie is still the follower.
		bk.watchSegmentMetadataBG(segmentZNodePath)
	}
}

// handleCron does the cron.
func (bk *Bookie) handleCron() {
	cronErr := error(nil)
	bk.scanAndSweepOnFS()
	cronErr = bk.updateBookieOnZK(bk.getStateOnZK())
	if cronErr != nil {
		bk.logger.Errorf("handleCron: Failed to update bookie on fs, %v", cronErr)
	}
}
