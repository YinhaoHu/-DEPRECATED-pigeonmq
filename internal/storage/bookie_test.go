package storage

import (
	"bytes"
	"fmt"
	"github.com/go-zookeeper/zk"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"pigeonmq/internal/testutil"
	"regexp"
	"testing"
	"time"
)

// TestLocalFileSystem tests the functionality of file system related methods of bookie
// in a single node and in a single go-routine.
//
// Includes: segmentHeader, append, appendAt, read, close.
func TestLocalFileSystem(t *testing.T) {
	cfg, cfgErr := NewConfig(testutil.TestConfigFilePath)
	testutil.CheckErrorAndFatalAsNeeded(cfgErr, t)

	tb := NewTestBookie(cfg, TestBookieLocationSingle)
	tc := testutil.NewTestCase("LocalFileSystem", t)
	tc.Begin()
	defer tc.Done()

	nIter := int64(32)
	entrySize := int64(4096)
	tc.CheckAssumption("SegmentMaxSize", func() bool {
		return nIter*entrySize <= tb.cfg.SegmentMaxSize-segmentHeaderSize
	})

	// Create segment.
	err := tb.bk.createSegmentOnFS(testutil.TestSegmentName)
	testutil.CheckErrorAndFatalAsNeeded(err, t)

	segmentEntries := make([][]byte, 0)
	segmentPositions := make([]int64, 0)
	// Append many entries in the segment without exceeding the size limit.
	tc.StartProgressBar(nIter)
	for i := int64(0); i < nIter; i++ {
		entryData := testutil.GenerateData(entrySize)
		segmentEntries = append(segmentEntries, entryData)
		pos, appErr := tb.bk.appendSegmentOnFS(testutil.TestSegmentName, entryData)
		testutil.CheckErrorAndFatalAsNeeded(appErr, t)
		segmentPositions = append(segmentPositions, pos)
		tc.RefreshProgressBar(i)
	}
	tc.EndProgressBar()

	// Check read.
	for i := int64(0); i < nIter; i++ {
		readData, nRead, readErr := tb.bk.readSegmentOnFS(testutil.TestSegmentName, segmentPositions[i], int64(len(segmentEntries[i])))
		if bytes.Compare(readData, segmentEntries[i]) != 0 {
			t.Fatalf("read content error : %v != %v", readData, segmentEntries[i])
		}
		if nRead != int64(len(segmentEntries[i])) {
			t.Fatalf("read size error : %v != %v", readData, segmentEntries[i])
		}
		testutil.CheckErrorAndFatalAsNeeded(readErr, t)
	}

	// Check payload.
	hdr := tb.bk.getSegmentHeaderOnFS(testutil.TestSegmentName)
	expectedHdr := segmentHeader{
		payloadSize: func() int64 {
			sum := int64(0)
			for _, b := range segmentEntries {
				sum += int64(len(b))
			}
			return sum
		}(),
		state: segmentStateOpen,
	}
	if hdr != expectedHdr {
		t.Fatalf("header error : %v != %v", hdr, expectedHdr)
	}

	// Fill the segment. Expect error.
	bigData := testutil.GenerateDataFast(tb.cfg.SegmentMaxSize - segmentHeaderSize)
	_, appErr := tb.bk.appendSegmentOnFS(testutil.TestSegmentName, bigData)
	if appErr == nil {
		t.Fatalf("should have failed")
	}

	// Fill the segment. Expect no error.
	appErr = tb.bk.appendAtSegmentOnFS(testutil.TestSegmentName, bigData, segmentHeaderSize, tb.bk.getSegmentOnFS(testutil.TestSegmentName))
	if appErr != nil {
		t.Fatalf("appErr : %v", appErr)
	}

	// Check header state before and after closing segment.
	hdr = tb.bk.getSegmentHeaderOnFS(testutil.TestSegmentName)
	expectedHdr = segmentHeader{
		payloadSize: tb.cfg.SegmentMaxSize - segmentHeaderSize,
		state:       segmentStateOpen,
	}
	if hdr != expectedHdr {
		t.Fatalf("header error : %v != %v", hdr, expectedHdr)
	}

	closeErr := tb.bk.closeSegmentOnFS(testutil.TestSegmentName)
	testutil.CheckErrorAndFatalAsNeeded(closeErr, t)
	hdr = tb.bk.getSegmentHeaderOnFS(testutil.TestSegmentName)
	expectedHdr = segmentHeader{
		payloadSize: tb.cfg.SegmentMaxSize - segmentHeaderSize,
		state:       segmentStateClose,
	}
	if hdr != expectedHdr {
		t.Fatalf("header error : %v != %v", hdr, expectedHdr)
	}
}

// TestCluster tests the concurrency correctness, fault-tolerance and scalability of
// a single Bookie Ensemble.
func TestCluster(t *testing.T) {
	// What have been tested so far?
	//  The basic fault-tolerance: follower leaves and re-joins.
	//  Follower election.
	//  The scalability is not tested, but theoretically it is correct.
	//  Basic create primary/backup segment, read and append.
	// TODO(Hoo-Future): Test more with consideration below,
	//  [test convenience]
	// 		-> Is there any approach to make integrate the shell assisted bookie restart into the same
	//  	process for convenient testing?
	//  	-> If the answer for the question above is NO, consider make the configuration file specification
	//      more convenient, manually modify the config is too bad.
	//  [test content]
	// 		-> Fault-tolerance can be satisfied by out goal requirement?
	//		-> Scalability and load balance is really correct?(Although it seems right theoretically)
	//		-> Concurrency test: with distributed lock.
	
	tc := testutil.NewTestCase("Cluster", t)
	tc.Begin()
	defer tc.Done()
	tc.CheckAssumption("test directory check", func() bool {
		realPathCmd := exec.Command("pwd")
		realPathBytes, err := realPathCmd.Output()
		testutil.CheckErrorAndFatalAsNeeded(err, t)
		matched, err := regexp.Match(".*/pigeonmq/internal/storage", realPathBytes)
		testutil.CheckErrorAndFatalAsNeeded(err, t)
		return matched
	})

	segName := testutil.TestSegmentName
	segmentCreated := false
	// Connect to the zk servers so that some data can be clean.
	zkOutput, openErr := os.OpenFile("testdata/zk.log", os.O_CREATE|os.O_RDWR, 0666)
	_ = zkOutput.Truncate(0)
	testutil.CheckErrorAndFatalAsNeeded(openErr, t)
	zkOptionSetLogger := func(conn *zk.Conn) {
		zkLogger := &testutil.TestZKPrinter{Out: zkOutput}
		conn.SetLogger(zkLogger)
	}
	zkConn, _, zkErr := zk.Connect([]string{"127.0.0.1:18001"}, 3*time.Second, zkOptionSetLogger)

	// NOTE: You might see an ERROR log entry in the bookie's log which is like "[ERROR] Failed to get the responsible
	// bookies for this segment". The error is intended to happen because the zk first delete the segment znode and
	// then shutdown the cluster. In the time window, the leaderCrash event will be sent.
	defer func() {
		if !segmentCreated {
			return
		}
		segZNodePath := filepath.Join(zkSegmentsPath, segName)
		zkErr = ZKDeleteAll(zkConn, segZNodePath)
		if zkErr != nil {
			fmt.Println("zkDeleteAllErr : ", zkErr)
		}
	}()
	testutil.CheckErrorAndFatalAsNeeded(zkErr, t)

	// Set up the cluster configuration.
	cfgTemplate, cfgErr := NewConfig(testutil.TestConfigFilePath)
	testutil.CheckErrorAndFatalAsNeeded(cfgErr, t)
	cluster := NewTestBookieCluster(t)
	bookiePorts := []int{19001, 19002}
	for _, port := range bookiePorts {
		cluster.Join(cfgTemplate, port)
	}
	cluster.runBookieSh("bookie_3", "start")
	// Wait for bookie_3 to start.
	time.Sleep(300 * time.Millisecond)
	cluster.StartAll()
	defer func() {
		cluster.runBookieSh("bookie_3", "stop")
		cluster.TearDownAll()
		// Wait for the cluster is actually close before saying the test is done.
		time.Sleep(1 * time.Second)
	}()

	bookieIPAddress := "127.0.0.1"
	bookies := make([]*rpc.Client, 3)

	makeRPCClient := func(bookieID int, port int) {
		rpcErr := error(nil)
		bookieAddress := fmt.Sprintf("%v:%v", bookieIPAddress, port)
		rpcPath := RPCPath(bookieAddress)
		bookies[bookieID], rpcErr = rpc.DialHTTPPath("tcp", bookieAddress, rpcPath)
		tc.WaitForCheckIfNeed(rpcErr)
		testutil.CheckErrorAndFatalAsNeeded(rpcErr, t)
	}

	for i, port := range bookiePorts {
		makeRPCClient(i, port)
	}
	defer func() {
		for _, cli := range bookies {
			err := cli.Close()
			if err != nil {
				fmt.Printf("rpcClient close error: %v", err)
				return
			}
		}
	}()

	// Scenario : Create primary segment, append to it and checks whether this can be
	// read from the peers.
	// membership: b0-> leader
	rpcCallWithoutError := func(whichBookie int, method string, args interface{}, reply interface{}) {
		serviceMethod := fmt.Sprintf("Bookie.%s", method)
		err := bookies[whichBookie].Call(serviceMethod, args, reply)
		if err != nil {
			fmt.Printf("Bookie %v call error. Error is listed below.\n", whichBookie)
		}
		testutil.CheckErrorAndFatalAsNeeded(err, t)
	}

	// Create primary segment
	cpsArgs := &CreatePrimarySegmentArgs{
		SegmentName: segName,
		Timeout:     1 * time.Second,
	}
	rpcCallWithoutError(0, "CreatePrimarySegment", cpsArgs, nil)
	segmentCreated = true

	cluster.runBookieSh("bookie_3", "stop")
	downTime := time.Now()

	// Append to leader.
	appendData := make([][]byte, 0)
	appendEntriesBeginPos := make([]int64, 0)
	numEntries := 4
	entrySize := int64(1024)
	for i := 0; i < numEntries; i++ {
		entry := testutil.GenerateData(entrySize)
		appendData = append(appendData, entry)
		apArgs := AppendPrimarySegmentArgs{
			SegmentName: segName,
			Data:        entry,
			Timeout:     3 * time.Second,
		}
		apReply := &AppendPrimarySegmentReply{}
		rpcCallWithoutError(0, "AppendPrimarySegment", apArgs, apReply)
		appendEntriesBeginPos = append(appendEntriesBeginPos, apReply.BeginPos)
	}

	cluster.runBookieSh("bookie_3", "start")
	comeTime := time.Now()
	if comeTime.Sub(downTime) > cfgTemplate.PermissibleDowntime {
		t.Errorf("append time is too long %v", downTime)
	}
	// Wait for the leader to append data to the new follower.
	time.Sleep(cfgTemplate.PermissibleDowntime + 1*time.Second)

	// Read from ...
	checkRead := func(bookieID int) {
		for i := 0; i < numEntries; i++ {
			readArgs := &ReadSegmentArgs{
				SegmentName: segName,
				BeginPos:    appendEntriesBeginPos[i],
				MaxSize:     int64(len(appendData[i])),
			}
			readReply := &ReadSegmentReply{}
			rpcCallWithoutError(bookieID, "ReadSegment", readArgs, readReply)
			want := appendData[i]
			get := readReply.Data
			if len(get) != len(want) {
				t.Fatalf("Get from bookie %v length error: %v != %v", bookieID, len(get), len(want))
			}
			if bytes.Compare(get, want) != 0 {
				t.Fatalf("Get from bookie %v error: %v != %v", bookieID, string(get), string(want))
			}
		}
	}

	// Give followers sometime to follow up.
	time.Sleep(1 * time.Second)

	makeRPCClient(2, 19003)
	for bookieID := range bookies {
		checkRead(bookieID)
	}
}
