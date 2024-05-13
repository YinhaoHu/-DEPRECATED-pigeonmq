package service

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"path"
	"pigeonmq/internal/util"
)

const (
	zkBrokersPath = "/brokers"
	zkTopicsPath  = "/topics"
)

// BrokerZKLogger represents the type for zk package to print log with.
type BrokerZKLogger struct {
	logger *util.Logger
}

func (l *BrokerZKLogger) Printf(str string, v ...interface{}) {
	zkMsg := fmt.Sprintf(str, v...)
	fullMsg := fmt.Sprintf("ZooKeeper %v", zkMsg)
	l.logger.Infof(fullMsg)
}

// initZK initializes the zookeeper connection and ephemeral znode in the zk brokers path directory.
func (b *Broker) initZK() {
	var err error

	// Connect to zk with options.
	zkOptionSetLogger := func(conn *zk.Conn) {
		zkLogger := &BrokerZKLogger{logger: b.logger}
		conn.SetLogger(zkLogger)
	}
	b.zk, _, err = zk.Connect(b.cfg.ZooKeeperClusterAddresses, b.cfg.ZooKeeperConnectionTimeout, zkOptionSetLogger)
	if err != nil {
		b.logger.Fatalf("Failed to connect to zookeeper cluster: %v", err)
	}

	// Create ephemeral sequence znode for this broker.
	_, err = b.zk.Create(zkBrokersPath, b.getZKState().ToBytes(), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		b.logger.Fatalf("Failed to create zookeeper broker: %v", err)
	}
}

// createTopic creates a topic znode under the topics path whose name is name.
func (b *Broker) createTopic(name string) error {
	topicPath := path.Join(zkTopicsPath, name)
	_, err := b.zk.Create(topicPath, nil, zk.FlagSequence|zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		b.logger.Errorf("Failed to create topic: %v", err)
		return err
	}
	return nil
}

// createPartition creates a partition under the topic.
func (b *Broker) createPartition(topic string, partition string) error {
	partitionPath := path.Join(zkTopicsPath, topic, partition)
	_, err := b.zk.Create(partitionPath, nil, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		b.logger.Errorf("Failed to create partition %v in topic %v: %v", partitionPath, topic, err)
	}
	return nil
}

type BrokerZNode struct {
	Address string
}

func (bz *BrokerZNode) ToBytes() []byte {
	bytes := []byte(bz.Address)
	return bytes
}

func (b *Broker) getZKState() *BrokerZNode {
	znode := &BrokerZNode{
		b.address,
	}
	return znode
}
