package service

import (
	"bufio"
	"os"
	"pigeonmq/internal/util"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ZooKeeperClusterAddresses  []string      // ZooKeeper cluster addresses
	ZooKeeperConnectionTimeout time.Duration // Timeout for connecting zk in seconds.
	IPAddress                  string        // The IP address the bookie will serve.
	Port                       int           // The port the bookie will serve for rpc.

	LogFileDirPath string // The path of the log directory.

	BookieSegmentMaxSize int64 // The max size of a Bookie segment.
}

// NewConfig generates a configuration from filepath.
func NewConfig(filepath string) *Config {
	cfg := new(Config)

	configFile, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer configFile.Close()
	scanner := bufio.NewScanner(configFile)
	for {
		key, value, err2 := util.GetOneConfig(scanner)
		if err2 != nil {
			panic(err2)
		}
		switch key {
		case "ZooKeeperClusterAddresses":
			cfg.ZooKeeperClusterAddresses = strings.Split(value, ";")
		case "ZooKeeperConnectionTimeout":
			cfg.ZooKeeperConnectionTimeout, err2 = time.ParseDuration(value)
		case "IPAddress":
			cfg.IPAddress = value
		case "Port":
			cfg.Port, err2 = strconv.Atoi(value)
		case "LogFileDirPath":
			cfg.LogFileDirPath = value
		case "BookieSegmentMaxSize":
			cfg.BookieSegmentMaxSize, err2 = strconv.ParseInt(value, 10, 64)
		}

		if err2 != nil {
			panic(err2)
		}
	}

	return cfg
}
