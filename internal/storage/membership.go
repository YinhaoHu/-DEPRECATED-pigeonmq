package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// electSegmentLeader implements the segment leader election algorithm with the
// znode name, bookie znode mapping on the segment znode.
func (bk *Bookie) electSegmentLeader(bookies map[string]*segmentBookieZNode) (leaderAddress string, err error) {
	if len(bookies) == 0 {
		return "", errors.New("bookie.electSegmentLeader: len(bookies) == 0")
	}
	// Select the highest commit offset bookie with the lowest zk sequential number.
	leader := func() string {
		for a, _ := range bookies {
			return a
		}
		return ""
	}()
	for znodeName, bookie := range bookies {
		if bookie.offset > bookies[leader].offset {
			leader = znodeName
		} else if bookie.offset == bookies[leader].offset && strings.Compare(leader, znodeName) > 0 {
			leader = znodeName
		}
	}
	return bookies[leader].address, nil
}

// ByResourceUtilization represents the sortable slice of BookieZKState.
type ByResourceUtilization []*BookieZKState

func (s ByResourceUtilization) Len() int { return len(s) }

func (s ByResourceUtilization) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ByResourceUtilization) Less(i, j int) bool {
	iUsedRatio := float64(s[i].StorageUsed/(s[i].StorageFree) + s[i].StorageUsed)
	jUsedRatio := float64(s[j].StorageUsed/(s[j].StorageFree) + s[j].StorageFree)
	return iUsedRatio < jUsedRatio
}

// selectSegmentFollower selects n new bookies whose free storage is not less than minFree as followers.
// returns n selected follower's address.
func (bk *Bookie) selectSegmentFollower(bookies []*BookieZKState, segmentBookies map[string]*segmentBookieZNode,
	n int32, minFree int64) (addresses []string, err error) {
	// If some bookies free storage is 0, delete them to avoid `divide by zero`.
	for _, bookie := range bookies {
		if bookie.StorageFree == 0 {
			bookie.StorageFree = 1
		}
	}

	// sort bookies based on the resource utilization.
	sort.Sort(ByResourceUtilization(bookies))

	// remember the addresses of the current followers in the segment.
	currentFollowerAddresses := make(map[string]struct{})
	if segmentBookies != nil {
		for _, bookie := range segmentBookies {
			currentFollowerAddresses[bookie.address] = struct{}{}
		}
	}

	// select followers now.
	selectedFollowers := make([]string, 0, n)
	for _, znode := range bookies {
		_, exists := currentFollowerAddresses[znode.Address]
		if !exists && znode.StorageFree >= minFree {
			selectedFollowers = append(selectedFollowers, znode.Address)
			n--
		}
		if n == 0 {
			break
		}
	}
	if n != 0 {
		return nil, fmt.Errorf("no enough bookies can be selected as followers with constraint "+
			"n=%v,minFree=%v", n, minFree)
	}
	return selectedFollowers, nil
}
