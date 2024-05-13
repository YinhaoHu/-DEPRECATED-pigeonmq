package service

import (
	"errors"
	"github.com/go-zookeeper/zk"
	"strconv"
)

type CreateTopicArgs struct {
	name string // The name of the topic.
}

// CreateTopic creates a topic in the brokers.
func (b *Broker) CreateTopic(args *CreateTopicArgs, _ *struct{}) error {
	var err error
	b.logger.Infof("Broker received create topic request: %s", args.name)
	defer b.logger.Infof("Broker responded create topic request: %s, err: %v", args.name, err)

	err = b.createTopic(args.name)
	return err
}

type PartitionMode int32

const (
	PartitionModeRW PartitionMode = iota // Partition is able to read/write initially.
	PartitionModeRO                      // Partition is only able to read initially.
)

type CreatePartitionsArgs struct {
	topic      string          // The name of the topic to which the partition belongs.
	partitions []int32         // The partitions needed to be created.
	modes      []PartitionMode // The modes for partitions initially.
}

// CreatePartitions creates some partitions in the given topic on this broker.
func (b *Broker) CreatePartitions(args *CreatePartitionsArgs, _ *struct{}) error {
	var err error
	b.logger.Infof("Broker received create partitions request: %s", args.topic)
	defer b.logger.Infof("Broker responded create partitions request: %s, err: %v", args.topic, err)

	// Create partitions on the zk.
	for _, partition := range args.partitions {
		partitionStr := strconv.Itoa(int(partition))
		err = b.createPartition(args.topic, partitionStr)
		if err != nil {
			if errors.Is(err, zk.ErrNodeExists) {
				// Existed partition is considered to be a correctness.
				continue
			}
			b.logger.Warnf("Failed to create partition %s: %v", partitionStr, err)
			return err
		}
	}

	return nil
}

type PublishMessageArgs struct {
	topic     string
	partition int32
	msg       []byte
}

func (b *Broker) PublishMessage(args *PublishMessageArgs, _ *struct{}) error {
	return nil
}

type PullMessageArgs struct {
	topic     string // The topic to pull messages from.
	partition int32  // The partition of the topic to pull messages.
	number    int32  // The number of maximum messages to pull.
}

type PullMessageReply struct {
	messages [][]byte // Messages no more than number specified in the pull message args.
}

func (b *Broker) PullMessage(args *PullMessageArgs, reply *PullMessageReply) error {
	return nil
}
