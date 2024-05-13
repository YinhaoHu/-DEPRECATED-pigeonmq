package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"
)

const (
	segmentHeaderSize = int(unsafe.Sizeof(segmentHeader{})) // Size of segment header in byte.
)

// segmentState represents the state of a segment.
type segmentState int16

const (
	segmentStateOpen segmentState = iota
	segmentStateClose
)

// segmentHeader implements the representation of the header in the segment file.
type segmentHeader struct {
	payloadSize int
	state       segmentState
}

// newSegmentHeader returns a segmentHeader from bytes.
func newSegmentHeader(bytes []byte) *segmentHeader {
	sh := new(segmentHeader)
	sh.payloadSize = int(binary.BigEndian.Uint32(bytes[:unsafe.Sizeof(sh.payloadSize)]))
	sh.state = segmentState(binary.BigEndian.Uint16(bytes[unsafe.Sizeof(sh.payloadSize):unsafe.Sizeof(*sh)]))
	return sh
}

// toBytes converts the segment header to bytes.
func (sh *segmentHeader) toBytes() []byte {
	bytes := make([]byte, segmentHeaderSize)
	binary.BigEndian.PutUint32(bytes[0:unsafe.Sizeof(0)], uint32(sh.payloadSize))
	binary.BigEndian.PutUint16(bytes[unsafe.Sizeof(0):unsafe.Sizeof(*sh)], uint16(sh.state))
	return bytes
}

// initFileSystem initializes the file system part setting of bookie.
func (bk *Bookie) initFileSystem() error {
	err := error(nil)

	// Set the resource usage.
	bk.storageUsed, err = getDirectoryFilesSize(bk.cfg.StorageDirectoryPath)
	if err != nil {
		return fmt.Errorf("initFileSystem: %w", err)
	}
	bk.storageFree = bk.cfg.StorageMaxSize - bk.storageUsed

	return nil
}

// createSegmentOnFS creates a segment file in the storage directory.
func (bk *Bookie) createSegmentOnFS(segmentName string) error {
	// Create the segment file.
	file := bk.getSegmentOnFS(segmentName)

	// Preallocate space for the segment based on the configured segment maximum size.
	err := file.Truncate(int64(bk.cfg.SegmentMaxSize))
	if err != nil {
		return fmt.Errorf("failed to preallocate space for segment file: %w", err)
	}

	// Write an initial header to the file.
	header := segmentHeader{0, segmentStateOpen}
	_, err = file.WriteAt(header.toBytes(), 0)
	if err != nil {
		return fmt.Errorf("failed to write initial header to segment file: %w", err)
	}

	// Update local resource information.
	bk.storageUsed += bk.cfg.SegmentMaxSize
	bk.storageFree -= bk.cfg.SegmentMaxSize

	return nil
}

// readSegmentOnFS reads no more than maxSize bytes of data from beginPos in the segment file.
func (bk *Bookie) readSegmentOnFS(segmentName string, beginPos int, maxSize int) (data []byte, n int, err error) {
	// Open the segment file for reading.
	file := bk.getSegmentOnFS(segmentName)

	// Read the current header to determine the size of the existing payload.
	headerBytes := make([]byte, segmentHeaderSize)
	_, err = file.ReadAt(headerBytes, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read header from segment file: %w", err)
	}
	header := newSegmentHeader(headerBytes)

	// Read the data from the file, limited to the specified maximum size.
	readSize := min(header.payloadSize-(beginPos-segmentHeaderSize), maxSize)
	buffer := make([]byte, readSize)
	size, err := file.ReadAt(buffer, int64(beginPos))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read data from segment file: %w", err)
	}

	// Return the actually read data and its size.
	return buffer[:size], size, nil
}

// appendSegmentOnFS appends data to the end of segment file.
func (bk *Bookie) appendSegmentOnFS(segmentName string, data []byte) (dataBeginPos int, err error) {
	file := bk.getSegmentOnFS(segmentName)

	// Read the current header to determine the size of the existing payload.
	headerBytes := make([]byte, segmentHeaderSize)
	_, err = file.ReadAt(headerBytes, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to read header from segment file: %w", err)
	}
	header := newSegmentHeader(headerBytes)
	newDataBeginPos := (segmentHeaderSize) + header.payloadSize

	err = bk.appendAtSegmentOnFS(segmentName, data, newDataBeginPos, file)

	return newDataBeginPos, err
}

func (bk *Bookie) appendAtSegmentOnFS(segmentName string, data []byte, off int, file *os.File) (err error) {
	if file == nil {
		file = bk.getSegmentOnFS(segmentName)
	}
	dataLen := len(data)
	// Check if appending the new data will exceed the maximum segment size.
	if off+dataLen > bk.cfg.SegmentMaxSize {
		return fmt.Errorf("appending data exceeds maximum segment size")
	}

	// Append the provided data to the end of the file.
	_, err = file.WriteAt(data, int64(off)) // Append to the end of the payload
	if err != nil {
		return fmt.Errorf("failed to append to the end of segment file: %w", err)
	}

	// Update the header with the new length of the payload.
	hdr := segmentHeader{
		payloadSize: off + dataLen - (segmentHeaderSize),
		state:       0,
	}
	_, err = file.WriteAt(hdr.toBytes(), 0)
	if err != nil {
		return fmt.Errorf("failed to update header in segment file: %w", err)
	}
	return nil
}

// closeSegmentOnFS set the state of segment to close.
func (bk *Bookie) closeSegmentOnFS(segmentName string) error {
	// Open the segment.
	segmentFile := bk.getSegmentOnFS(segmentName)

	// Update header state.
	header := bk.getSegmentHeaderOnFS(segmentName)
	header.state = segmentStateClose
	if _, err := segmentFile.WriteAt(header.toBytes(), 0); err != nil {
		return fmt.Errorf("closeSegmentOnFS: %w", err)
	}

	return nil
}

// removeSegmentOnFS removes the segment from the file system.
func (bk *Bookie) removeSegmentOnFS(segmentName string) error {
	segmentPath := filepath.Join(bk.cfg.StorageDirectoryPath, segmentName)
	return os.Remove(segmentPath)
}

// getSegmentHeaderOnFS returns the segment header.
func (bk *Bookie) getSegmentHeaderOnFS(segmentName string) segmentHeader {
	file := bk.getSegmentOnFS(segmentName)

	headerBytes := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		bk.logger.Fatalf("getSegmentHeaderOnFS: failed to read header from segment file: %v", err)
		return segmentHeader{}
	}
	header := newSegmentHeader(headerBytes)
	return *header
}

// getDirectoryFilesSize get the total size of all files in the directory without recursive search.
func getDirectoryFilesSize(dirName string) (size int, err error) {
	var totalSize int64

	// Open the directory
	dir, err := os.Open(dirName)
	if err != nil {
		return 0, err
	}

	// Read all directory entries
	entries, err := dir.Readdir(-1)
	if err != nil {
		return 0, err
	}

	// Iterate over each entry
	for _, entry := range entries {
		// Check if it's a regular file
		if entry.Mode().IsRegular() {
			// Add the size of the file to the total
			totalSize += entry.Size()
		}
	}

	err = dir.Close()
	if err != nil {
		return 0, err
	}
	return int(totalSize), nil
}

func (bk *Bookie) scanAndSweepOnFS() {
	storageDir := bk.cfg.StorageDirectoryPath
	retentionHours := bk.cfg.RetentionHours
	currentTime := time.Now()

	// Walk through the storage directory
	err := filepath.Walk(storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			bk.logger.Errorf("Error accessing file %s: %s\n", path, err)
			return nil
		}

		if info.IsDir() {
			return nil
		}

		segmentFile, err := os.Open(path)
		if err != nil {
			bk.logger.Errorf("Error opening file %s: %s\n", path, err)
			return nil
		}

		segmentHeaderBytes := make([]byte, segmentHeaderSize)
		_, err = segmentFile.ReadAt(segmentHeaderBytes, 0)
		if err != nil {
			bk.logger.Errorf("Error reading header from file %s: %s\n", path, err)
		}
		segmentBirth := info.ModTime()
		_, segmentIsNeeded := bk.segments[filepath.Base(path)]
		// If file is closed before retentionHours OR
		// the file payload size is not needed anymore because
		// this bookie crashed before and comes back after permissibleDownTime,
		// there is another bookie comes to take over this segment.
		if (currentTime.Sub(segmentBirth) >= retentionHours) || (!segmentIsNeeded) {
			err = os.Remove(path)
			if err != nil {
				bk.logger.Errorf("Error removing file %s: %s\n", path, err)
				return err
			}
			bk.storageUsed -= bk.cfg.SegmentMaxSize
			bk.storageFree += bk.cfg.SegmentMaxSize
		}

		return nil
	})

	if err != nil {
		bk.logger.Errorf("scanAndSweepOnFS error: %s\n", err)
	}
}

func (bk *Bookie) scanAndCloseSegmentFDs() {
	// TODO: Need?
}

func (bk *Bookie) getSegmentOnFS(segmentName string) *os.File {
	err := error(nil)
	seg, exist := bk.openSegmentFiles[segmentName]
	if !exist {
		bk.openSegmentFiles[segmentName], err = os.OpenFile(filepath.Join(bk.cfg.StorageDirectoryPath, segmentName),
			os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			bk.logger.Fatalf("Error opening file %s: %s\n", segmentName, err)
		}
		seg = bk.openSegmentFiles[segmentName]
	}
	return seg
}
