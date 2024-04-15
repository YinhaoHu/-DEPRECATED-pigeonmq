// Package testutil Package test util provides utilities for testing purposes.
package testutil

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"testing"
	"time"
)

const (
	TestConfigFilePath = "testdata/bookie-test.cfg"
	TestSegmentName    = "t-seg"
)

// CheckErrorAndFatalAsNeeded checks whether err is nil and fatal if not.
func CheckErrorAndFatalAsNeeded(err error, t *testing.T) {
	stackBuf := make([]byte, 1024)
	runtime.Stack(stackBuf, false)
	if err != nil {
		t.Fatalf("error : %v.\n\n%v", err, string(stackBuf))
	}
}

// GenerateRandomString generates a random string of the specified size in bytes,
// containing ASCII characters 'a'-'z', 'A'-'Z', and '0'-'9'.
func GenerateRandomString(size int64) string {
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

// GenerateData randomly generate a data which is 'size' bytes.
func GenerateData(size int64) []byte {
	str := GenerateRandomString(size)
	return []byte(str)
}

// GenerateDataFast randomly generate a data which is 'size' bytes but very fast.
func GenerateDataFast(size int64) []byte {
	data := make([]byte, size)
	return data
}

// TestCase represents a test case.
type TestCase struct {
	name          string
	beginTime     time.Time
	totalProgress int64
	test          *testing.T
}

// NewTestCase creates a new test case with the given name and testing.T instance.
func NewTestCase(name string, t *testing.T) *TestCase {
	c := new(TestCase)
	c.test = t
	c.name = name
	return c
}

// Begin marks the beginning of the test case.
func (t *TestCase) Begin() {
	t.beginTime = time.Now()
	fmt.Printf("Testing: %v...\n", t.name)
}

// Done marks the end of the test case.
func (t *TestCase) Done() {
	if t.totalProgress != 0 {
		t.EndProgressBar()
	}
	if !t.test.Failed() {
		usedTime := time.Now().Sub(t.beginTime)
		fmt.Printf("\tPASS	%.2fs\n", float64(usedTime.Milliseconds())/float64(1000))
	}
}

// StartProgressBar starts a progress bar with the given total progress.
func (t *TestCase) StartProgressBar(n int64) {
	t.totalProgress = n
	t.RefreshProgressBar(0)
}

// CheckAssumption checks a condition and fails the test case if the condition is false.
func (t *TestCase) CheckAssumption(name string, condition func() bool) {
	if !condition() {
		t.test.Fatalf("Assumption %v failed for %v", name, t.name)
	}
}

// RefreshProgressBar refreshes the progress bar with the current progress.
func (t *TestCase) RefreshProgressBar(progress int64) {
	fmt.Printf("\r progress: %.2f%%", 100*float64(progress)/float64(t.totalProgress))
}

// EndProgressBar ends the progress bar.
func (t *TestCase) EndProgressBar() {
	fmt.Printf("\r%64s\r", " ")
	t.totalProgress = 0
}
