package service

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"os"
	"path/filepath"
	"pigeonmq/internal/util"
)

type Broker struct {
	zk *zk.Conn

	cfg     *Config
	address string

	logger *util.Logger
}

// NewBroker generates a new broker instance.
func NewBroker(cfg *Config) *Broker {
	broker := new(Broker)

	// Set the configuration.
	broker.cfg = cfg

	// Initialize the logger.
	logFileBase := fmt.Sprintf("broker-%v-%v", cfg.Port, os.Getpid())
	logFilePath := filepath.Join(broker.cfg.LogFileDirPath, logFileBase)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	broker.logger = util.NewLogger(logFile, util.DebugLevel)

	// Initialize ZooKeeper.
	broker.initZK()

	return broker
}

// Run runs the broker server. Typically, run it in a go statement.
func (b *Broker) Run() {

}

// Shutdown shutdowns the broker server.
func (b *Broker) Shutdown() {
	b.zk.Close()
}
