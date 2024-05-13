package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"pigeonmq/internal/util"
	"strconv"
	"strings"
	"time"
)

// Config represents the configuration information of a bookie.
type Config struct {
	ReplicateNumber      int32 // Number of replicas of a segment
	MinimumReplicaNumber int32 // Minimum number of replicas of an append-available segment.

	SegmentMaxSize       int    // Max size of a segment.
	StorageMaxSize       int    // Max size of storage can be used by the bookie in fs.
	StorageDirectoryPath string // Storage directory path in fs.
	LogFilePath          string // Log file path in fs.

	PermissibleDowntime             time.Duration // Time to wait for a follower to come back in a segment.
	ZooKeeperClusterAddresses       []string      // ZooKeeper cluster addresses
	ZooKeeperConnectionTimeout      time.Duration // Timeout for connecting zk in seconds.
	DefaultPeerCommunicationTimeout time.Duration // Default timeout for a bookie to communicate with other peers.
	IPAddress                       string        // The IP address the bookie will serve.
	Port                            int           // The port the bookie will serve for rpc.

	CronTimer      time.Duration // Specify the frequency of cron.
	RetentionHours time.Duration // The hours a segment should be kept for in persistence.
}

// NewConfig returns the configuration information of the bookie in the given file path.
func NewConfig(filePath string) (*Config, error) {
	cfg := &Config{} // Initialize a new Config object
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileScanner := bufio.NewScanner(file)
	for {
		key, v, getConfigErr := util.GetOneConfig(fileScanner)
		if getConfigErr != nil {
			return nil, getConfigErr
		}
		if key == "" {
			break
		}
		switch key {
		case "ReplicateNumber":
			replicateNumber, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.ReplicateNumber = int32(replicateNumber)
		case "MinimumReplicaNumber":
			minimumReplicaNumber, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.MinimumReplicaNumber = int32(minimumReplicaNumber)
		case "SegmentMaxSize":
			segmentMaxSize, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			cfg.SegmentMaxSize = int(segmentMaxSize)
		case "StorageMaxSize":
			storageMaxSize, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			cfg.StorageMaxSize = int(storageMaxSize)
		case "LogFilePath":
			cfg.LogFilePath = v
		case "ZooKeeperCluster":
			cfg.ZooKeeperClusterAddresses = strings.Split(v, ";")
		case "DefaultPeerCommunicationTimeout":
			timeoutSeconds, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.DefaultPeerCommunicationTimeout = time.Duration(timeoutSeconds) * time.Second
		case "ZooKeeperConnectionTimeout":
			zkConnectionTimeout, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			cfg.ZooKeeperConnectionTimeout = zkConnectionTimeout
		case "StorageDirectoryPath":
			cfg.StorageDirectoryPath = v
		case "IPAddress":
			cfg.IPAddress = v
		case "RetentionHours":
			retentionDays, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.RetentionHours = time.Duration(retentionDays) * time.Hour
		case "Port":
			port, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.Port = port
		case "PermissibleDowntime":
			permissibleDowntime, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			cfg.PermissibleDowntime = permissibleDowntime
		case "CronTimer":
			cronTimer, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			cfg.CronTimer = cronTimer
		default:
			return nil, fmt.Errorf("unexpected config key: %s", key)
		}
	}
	return cfg, nil
}

// toJsonString converts the config to a json format string.
func (cfg *Config) toJsonString() string {
	jsonBytes, _ := json.MarshalIndent(*cfg, "", "  ")
	return string(jsonBytes)
}
