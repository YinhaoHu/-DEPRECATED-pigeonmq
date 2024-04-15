package storage

import (
	"bytes"
	"pigeonmq/internal/testutil"
	"testing"
)

// TestLocalFileSystem tests the functionality of file system related methods of bookie
// in a single node and in a single go-routine.
//
// Includes: segmentHeader, append, appendAt, read, close.
func TestLocalFileSystem(t *testing.T) {
	tb := NewTestBookie(testutil.TestConfigFilePath, TestBookieLocationSingle)
	tc := testutil.NewTestCase("LocalFileSystem", t)
	tc.Begin()

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

func TestCluster(t *testing.T) {
	/* TODO(Hoo@4-16): Test Cluster with concurrency and low unreliability(maybe 35% packet loss) ? .
	Code should go like the following way.

	Start the three bookie servers. ( Maybe tc tool can be used for unreliability)

	bookie1RPC := rpc.DialHTTP("tcp", "127.0.0.1:19001")
	bookie2RPC := rpc.DialHTTP("tcp", "127.0.0.1:19002")
	bookie3RPC := rpc.DialHTTP("tcp", "127.0.0.1:19003")

	createPrimarySegmentArgs := &CreatePrimarySegmentArgs{}
	createPrimarySegmentReply := &CreatePrimarySegmentReply{}
	bookie1RPC.Call("Bookie.CreatePrimarySegment", args, reply)

	Close the three bookie servers. clear the state.
	*/
}
