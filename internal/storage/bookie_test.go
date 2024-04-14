package storage

import (
	bytes2 "bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	rand2 "math/rand"
	"os"
	"path/filepath"
	"pigeonmq/internal/testutil"
	"runtime"
	"syscall"
	"testing"
	"time"
)

const (
	testConfigFilename = "testdata/bookie-test.cfg"
	testSegmentName    = "t-seg"
)

// HELPER FUNCTIONS

func (bk *Bookie) _testReadFrom(address string, args *ReadSegmentArgs, reply *ReadSegmentReply, timeout time.Duration) error {
	return bk.sendRPC(address, "Bookie.ReadSegment", args, reply, timeout)
}

var bkGenerated bool = false
var testBookie *Bookie

func setup() (error, *Bookie) {
	if bkGenerated {
		return nil, testBookie
	}
	cfg, err := NewConfig(testConfigFilename)
	if err != nil {
		err = errors.Join(errors.New("ConfigInit"), err)
		return err, nil
	}

	testBookie, err = NewBookie(cfg)
	if err != nil {
		return err, nil
	}
	bkGenerated = true
	return nil, testBookie
}

func cleanup(bk *Bookie, t *testing.T, deleteSegmentOnLocal bool, deleteSegmentOnCluster bool) {
	if deleteSegmentOnLocal {
		err := bk.removeSegmentOnFS(testSegmentName)
		checkErrorAndFatalAsNeeded(err, t)
	}
	if deleteSegmentOnCluster {
		err := zkDeleteAll(bk.zkConn, filepath.Join(zkSegmentsPath, testSegmentName))
		checkErrorAndFatalAsNeeded(err, t)
		os.Remove(fmt.Sprintf("/home/hoo/Filebase/project/pigeonmq/cmd/pigeonmq-bookie/storage2/%v", testSegmentName))
		os.Remove(fmt.Sprintf("/home/hoo/Filebase/project/pigeonmq/cmd/pigeonmq-bookie/storage3/%v", testSegmentName))
	}
	bk.Close()
}

// setupCluster starts another n-1 processes.
func setupCluster(n int) error {
	// TODO(Hoo@future) : implement me.
	for i := 2; i <= n; i++ {
		configFile := fmt.Sprintf("/home/hoo/Filebase/project/pigeonmq/cmd/pigeonmq-bookie/bookie_%v.cfg", i)
		err := syscall.Exec("/home/hoo/Filebase/project/pigeonmq/cmd/pigeonmq-bookie/pigeonmq-bookie", []string{
			"", "--config", configFile}, syscall.Environ())
		if err != nil {
			fmt.Println("error while setting up cluster", err)
			return err
		}
	}
	return nil
}

func checkErrorAndFatalAsNeeded(err error, t *testing.T) {
	stackBuf := make([]byte, 1024)
	runtime.Stack(stackBuf, false)
	if err != nil {
		t.Fatalf("error : %v.\n\n%v", err, string(stackBuf))
	}
}

// generateRandomString generates a random string of the specified size in bytes,
// containing ASCII characters 'a'-'z', 'A'-'Z', and '0'-'9'.
func generateRandomString(size int64) string {
	// Define the set of valid characters
	validChars := "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzX0123456789"
	validCharsLen := len(validChars)

	// Generate random bytes
	randomBytes := make([]byte, size)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Handle error if generating random bytes fails
		fmt.Println("Error generating random bytes:", err)
		return ""
	}

	// Convert bytes to string
	randomString := ""
	for _, b := range randomBytes {
		randomString += string(validChars[int(b)%validCharsLen])
	}
	return randomString
}

// generateData randomly generate a data which is 'size' bytes.
func generateData(size int64) []byte {
	str := generateRandomString(size)
	return []byte(str)
}

// zkDeleteAll deletes all the children recursively in the path.
func zkDeleteAll(conn *zk.Conn, path string) error {
	children, _, err := conn.Children(path)
	if err != nil {
		return err
	}

	for _, child := range children {
		childPath := path + "/" + child
		err := zkDeleteAll(conn, childPath)
		if err != nil {
			return err
		}
	}

	err = conn.Delete(path, -1)
	if err != nil && !errors.Is(err, zk.ErrNoNode) {
		return err
	}

	return nil
}

// =====----------------------------
//
// 			TEST FUNCTIONS
//
// =====----------------------------

func TestBookie_LocalBasic(t *testing.T) {
	tc := testutil.NewTestCase("LocalBasic", t)
	tc.Begin()
	err, bk := setup()
	checkErrorAndFatalAsNeeded(err, t)
	checkErrorAndFatalAsNeeded(err, t)
	defer tc.Done()
	defer cleanup(bk, t, true, false)

	segment := testSegmentName
	// create segment
	err = bk.createSegmentOnFS(segment)
	checkErrorAndFatalAsNeeded(err, t)

	chunkSize := int64(bk.cfg.SegmentMaxSize-8) / 1024
	chunk := generateData(chunkSize)

	// append segment
	pos, err := bk.appendSegmentOnFS(segment, chunk)
	checkErrorAndFatalAsNeeded(err, t)
	if pos != segmentHeaderSize {
		t.Fatalf("wrong first pos. want %v, get %v", segmentHeaderSize, pos)
	}

	// read segment
	bytes, nRead, err := bk.readSegmentOnFS(segment, pos, chunkSize*2)
	checkErrorAndFatalAsNeeded(err, t)
	if nRead != chunkSize {
		t.Fatalf("read size not equal. get %v, want %v", nRead, chunkSize)
	}
	if bytes2.Compare(bytes, chunk) != 0 {
		t.Fatalf("read chunk not equal. get %v, want %v", bytes, chunk)
	}

	pos, err = bk.appendSegmentOnFS(segment, chunk)
	checkErrorAndFatalAsNeeded(err, t)
	if pos != segmentHeaderSize+chunkSize {
		t.Fatalf("wrong first pos. want %v, get %v", segmentHeaderSize+chunkSize, pos)
	}
}

func TestBookie_LocalComprehensive(t *testing.T) {
	tc := testutil.NewTestCase("LocalComprehensive", t)
	tc.Begin()
	err, bk := setup()
	checkErrorAndFatalAsNeeded(err, t)
	checkErrorAndFatalAsNeeded(err, t)
	defer cleanup(bk, t, true, false)

	segment := testSegmentName
	err = bk.createSegmentOnFS(segment)
	checkErrorAndFatalAsNeeded(err, t)

	readBeginPos := segmentHeaderSize
	nIter := int64(128)
	maxChunkSize := min(1024*1024, bk.cfg.SegmentMaxSize-segmentHeaderSize) / nIter
	tc.StartProgressBar(nIter)
	for i := int64(1); i < nIter; i++ {
		tc.RefreshProgressBar(i)
		chunkSize := maxChunkSize - (rand2.Int63n(100))
		chunk := generateData(chunkSize)
		readBeginPos, err = bk.appendSegmentOnFS(segment, chunk)
		checkErrorAndFatalAsNeeded(err, t)
		readChunk, _, err := bk.readSegmentOnFS(segment, readBeginPos, chunkSize*2)
		checkErrorAndFatalAsNeeded(err, t)
		if bytes2.Compare(readChunk, chunk) != 0 {
			// I assume the data should be identical if the size is identical.
			t.Fatalf("read error: read chunck size %v, want %v", len(readChunk), len(chunk))
		}
	}

	tc.Done()
}

func TestBookie_LocalSegmentHeader(t *testing.T) {
	tc := testutil.NewTestCase("LocalSegmentHeader", t)
	tc.Begin()
	err, bk := setup()
	checkErrorAndFatalAsNeeded(err, t)
	checkErrorAndFatalAsNeeded(err, t)
	defer func() {
		cleanup(bk, t, true, false)
		tc.Done()
	}()

	err = bk.createSegmentOnFS(testSegmentName)
	checkErrorAndFatalAsNeeded(err, t)

	expectedHeader := segmentHeader{0, segmentStateOpen}

	gotHeader := bk.getSegmentHeaderOnFS(testSegmentName)
	checkErrorAndFatalAsNeeded(err, t)
	if bytes2.Compare(gotHeader.toBytes(), expectedHeader.toBytes()) != 0 {
		t.Fatalf("wrong intial header. expected %+v, got %+v", expectedHeader, gotHeader)
	}
	dataSize := int64(1024)
	data := generateData(dataSize)
	beginPos, err := bk.appendSegmentOnFS(testSegmentName, data)
	checkErrorAndFatalAsNeeded(err, t)
	if beginPos != segmentHeaderSize {
		t.Fatalf("first chunk in a segment should begin with %v, actually %v",
			segmentHeaderSize, beginPos)
	}
	err = bk.closeSegmentOnFS(testSegmentName)
	checkErrorAndFatalAsNeeded(err, t)
	gotHeader = bk.getSegmentHeaderOnFS(testSegmentName)
	checkErrorAndFatalAsNeeded(err, t)
	expectedHeader = segmentHeader{dataSize, segmentStateClose}
	if bytes2.Compare(gotHeader.toBytes(), expectedHeader.toBytes()) != 0 {
		t.Fatalf("wrong header. expected %+v, got %+v", expectedHeader, gotHeader)
	}
}

func TestBookie_LocalZK(t *testing.T) {
	tc := testutil.NewTestCase("LocalZK", t)
	tc.Begin()
	err, bk := setup()
	checkErrorAndFatalAsNeeded(err, t)
	checkErrorAndFatalAsNeeded(err, t)
	defer tc.Done()
	defer cleanup(bk, t, false, false)

	// check the existence of bookie znode and segment znode.
	segmentZNodePath, bookieZNodePath, err := bk.createSegmentOnZK("seg1")
	checkErrorAndFatalAsNeeded(err, t)

	paths := []string{segmentZNodePath, bookieZNodePath}
	for _, path := range paths {
		exists, _, existsErr := bk.zkConn.Exists(path)
		checkErrorAndFatalAsNeeded(existsErr, t)
		if !exists {
			t.Fatalf("initZK: path %v does not exist", path)
		}
	}

	// clean the nodes.
	err = bk.zkConn.Delete(bookieZNodePath, -1)
	err = bk.zkConn.Delete(filepath.Join(segmentZNodePath, zkSegmentMetadataZNodeName), -1)
	err = bk.zkConn.Delete(segmentZNodePath, -1)
	checkErrorAndFatalAsNeeded(err, t)
}

func TestBookie_ClusterSegment(t *testing.T) {
	tc := testutil.NewTestCase("ClusterCreateSegment", t)
	tc.Begin()
	err, bk := setup()
	checkErrorAndFatalAsNeeded(err, t)
	defer tc.Done()
	defer cleanup(bk, t, true, false)

	args := &CreatePrimarySegmentArgs{testSegmentName, 3 * time.Second}
	err = bk.CreatePrimarySegment(args, nil)
	checkErrorAndFatalAsNeeded(err, t)

	segmentPath := filepath.Join(zkSegmentsPath, testSegmentName)
	children, _, err := bk.zkConn.Children(segmentPath)
	if err != nil {
		return
	}
	// Give peers some time to follow up.
	time.Sleep(1 * time.Second)

	// there should be three bookie znode and one metadata znode in segmentPath.
	expectedNChildren := bk.cfg.ReplicateNumber + 1
	if len(children) != expectedNChildren {
		t.Fatalf("wrong number of children at path %v, want len %v, but %v. children:%v",
			segmentPath, expectedNChildren, len(children), children)
	}
	peerStates, err := bk.getPeersOnZK()
	checkErrorAndFatalAsNeeded(err, t)
	for _, state := range peerStates {
		state.StorageUsed = state.StorageUsed / 1024 / 1024
		state.StorageFree = state.StorageFree / 1024 / 1024
		fmt.Printf("peer state(storage in MB): %+v\n", *state)
	}

	appendDataSize := int64(1024)
	appendData := generateRandomString(appendDataSize)
	appendArgs := &AppendPrimarySegmentArgs{testSegmentName, []byte(appendData), 3 * time.Second}
	appendReply := &AppendPrimarySegmentReply{}
	appendErr := bk.AppendPrimarySegment(appendArgs, appendReply)
	checkErrorAndFatalAsNeeded(appendErr, t)

	// Give peers some time to get the newest data.
	time.Sleep(1 * time.Second)

	readArgs := &ReadSegmentArgs{testSegmentName, segmentHeaderSize, appendDataSize * 2}
	readReply := &ReadSegmentReply{}
	readErr := bk._testReadFrom("127.0.0.1:19002", readArgs, readReply, 3*time.Second)
	checkErrorAndFatalAsNeeded(readErr, t)
	if bytes2.Compare(readReply.Data, []byte(appendData)) != 0 {
		t.Fatalf("error in appending data. \nget:%v\nwant:%v", string(readReply.Data), string(appendData))
	}
	readReply = &ReadSegmentReply{}
	readErr = bk._testReadFrom("127.0.0.1:19003", readArgs, readReply, 3*time.Second)
	checkErrorAndFatalAsNeeded(readErr, t)
	if bytes2.Compare(readReply.Data, []byte(appendData)) != 0 {
		t.Fatalf("error in appending data. \nget:%v\nwant:%v", string(readReply.Data), string(appendData))
	}
}
