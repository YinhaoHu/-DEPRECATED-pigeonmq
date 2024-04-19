package storage

import (
	"github.com/go-zookeeper/zk"
	"net/rpc"
	"path/filepath"
	"sync"
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
	segmentName      string   // The name of the involved segment.
	beforeFollowers  []string // ZNode names. The followers before this event happens.
	currentFollowers []string // ZNode names. The followers after this event happens.
}

func (bk *Bookie) sendSegmentToNewFollowers(segmentName string, newFollowers []string,
	segmentBookies map[string]*segmentBookieZNode) {
	bk.logger.Infof("sendSegmentToNewFollowers: segmentName=%v, newFollowers=%v", segmentName, newFollowers)
	wg := sync.WaitGroup{}
	for _, newFollower := range newFollowers {
		bookie := segmentBookies[newFollower]
		wg.Add(1)
		go func(bookie *segmentBookieZNode) {
			data, _, _ := bk.readSegmentOnFS(segmentName, bookie.offset, bk.cfg.SegmentMaxSize)
			appendBackupSegmentArgs := &AppendBackupSegmentArgs{segmentName, data, bookie.offset}
			rpcErr := bk.sendRPC(bookie.address, "Bookie.AppendBackupSegment", &appendBackupSegmentArgs,
				nil, 1*time.Second)
			if rpcErr != nil {
				bk.logger.Errorf("sendSegmentToNewFollowers: AppendBackupSegment RPC failed: %v,follower %v", rpcErr, bookie.address)
			}
			wg.Done()
		}(bookie)
	}
	wg.Wait()

	bk.logger.Infof("handleEventFollowerUpdate met a new follower comes into the bookie ensemble.")
}

// handleEventFollowerUpdate acts for a follower coming or leaving the segment bookie group
// which is leaded by this bookie.
func (bk *Bookie) handleEventFollowerUpdate(args *eventFollowerUpdateArgs) {
	// Watch this segment again.
	bk.watchSegmentPeersBG(filepath.Join(zkSegmentsPath, args.segmentName))

	// Get peers state on ZK.
	segmentPath := filepath.Join(zkSegmentsPath, args.segmentName)
	segmentBookies, err := bk.getSegmentBookiesOnZK(segmentPath)

	if err != nil {
		bk.logger.Errorf("handleEventFollowerUpdate, error %v", err)
		return
	}
	bk.logger.Infof("handleEventFollowerUpdate, segmentBookies %v(len=%v) ", segmentBookies, len(segmentBookies))

	if len(args.currentFollowers) < len(args.beforeFollowers) {
		// Follower crash event.
		if len(segmentBookies) == len(args.beforeFollowers)-1 {
			// The crashed followers come back.
			crashedFollowers := make([]string, 0)
			currentFollowersMap := make(map[string]bool)
			for _, follower := range args.currentFollowers {
				currentFollowersMap[follower] = true
			}
			for follower := range segmentBookies {
				if currentFollowersMap[follower] == false {
					crashedFollowers = append(crashedFollowers, follower)
				}
			}
			bk.logger.Infof("handleEventFollowerUpdate: crashed followers come back, they are %+v", crashedFollowers)
			// Sync segment with them.
			bk.sendSegmentToNewFollowers(args.segmentName, crashedFollowers, segmentBookies)
		} else {
			peerStates, err2 := bk.getPeersOnZK()
			if err2 != nil {
				bk.logger.Errorf("handleEventFollowerUpdate, error %v", err2)
				return
			}
			bk.logger.Infof("handleEventFollowerUpdate, %v peers are alive.", len(peerStates))
			// Select follower now.
			follower, err2 := bk.selectSegmentFollower(peerStates, segmentBookies, 1, bk.cfg.SegmentMaxSize)
			if len(follower) == 0 {
				bk.logger.Warnf("handleEventFollowerUpdate cannot find enough bookies as followers")
				return
			}
			if err2 != nil {
				bk.logger.Errorf("handleEventFollowerUpdate, error %v", err2.Error())
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
					err2 = doneCall.Error
					if err2 != nil {
						bk.logger.Errorf("Bookie.CreatePrimarySegment: error in calling CreateBackupSegment, err2=%v,follower=%v", err2, follower)
					}
				}
			}
		}
	} else if len(args.currentFollowers) > len(args.beforeFollowers) {
		// Follower join event.
		// Find the new followers.
		newFollowers := make([]string, 0)
		beforeFollowers := make(map[string]bool)
		for _, follower := range args.beforeFollowers {
			beforeFollowers[follower] = true
		}
		for follower := range segmentBookies {
			if _, ok := beforeFollowers[follower]; !ok {
				newFollowers = append(newFollowers, follower)
			}
		}
		bk.logger.Infof("handleEventFollowerUpdate new followers join, they are %+v", newFollowers)
		// Sync segment with them.
		bk.sendSegmentToNewFollowers(args.segmentName, newFollowers, segmentBookies)
	} else {
		// TODO(Hoo@Future): not considered.
		bk.logger.Warnf("Some follower crashed but comes back.")
	}

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
		go func() {
			followers := make([]string, 0)
			for follower := range segmentBookies {
				followers = append(followers, follower)
			}
			event := &Event{
				Description: "follower crash",
				eType:       eventFollowerUpdate,
				args:        eventFollowerUpdateArgs{filepath.Base(segmentZNodePath), followers, followers},
			}
			time.Sleep(bk.cfg.PermissibleDowntime)
			bk.eventCh <- event
			bk.logger.Infof("Bookie send follower crash event.")
		}()
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
