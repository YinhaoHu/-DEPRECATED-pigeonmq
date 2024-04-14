package testutil

import (
	"fmt"
	"testing"
	"time"
)

type TestCase struct {
	name          string
	beginTime     time.Time
	totalProgress int64
	test          *testing.T
}

func NewTestCase(name string, t *testing.T) *TestCase {
	c := new(TestCase)
	c.test = t
	c.name = name
	return c
}

func (t *TestCase) Begin() {
	t.beginTime = time.Now()
	fmt.Printf("Testing: %v...\n", t.name)
}

func (t *TestCase) Done() {
	if t.totalProgress != 0 {
		t.EndProgressBar()
	}
	if t.test.Failed() == false {
		usedTime := time.Now().Sub(t.beginTime)
		fmt.Printf("\tPASS	%.2fs\n", float64(usedTime.Milliseconds())/float64(1000))
	}
}

func (t *TestCase) StartProgressBar(n int64) {
	t.totalProgress = n
}

func (t *TestCase) RefreshProgressBar(progress int64) {
	fmt.Printf("\r progress: %.2f%%", 100*float64(progress)/float64(t.totalProgress))
}

func (t *TestCase) EndProgressBar() {
	fmt.Printf("\r%64s\r", " ")
	t.totalProgress = 0
}
