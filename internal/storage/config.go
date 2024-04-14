package storage

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config represents the configuration information of a bookie.
type Config struct {
	ReplicateNumber      int // Number of replicas of a segment
	MinimumReplicaNumber int // Minimum number of replicas of an append-available segment.

	SegmentMaxSize       int64  // Max size of a segment.
	StorageMaxSize       int64  // Max size of storage can be used by the bookie in fs.
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
		key, v, getConfigErr := getOneConfig(fileScanner)
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
			cfg.ReplicateNumber = replicateNumber
		case "MinimumReplicaNumber":
			minimumReplicaNumber, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.MinimumReplicaNumber = minimumReplicaNumber
		case "SegmentMaxSize":
			segmentMaxSize, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			cfg.SegmentMaxSize = segmentMaxSize
		case "StorageMaxSize":
			storageMaxSize, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, err
			}
			cfg.StorageMaxSize = storageMaxSize
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
			timeoutSeconds, err := strconv.Atoi(v)
			if err != nil {
				return nil, err
			}
			cfg.ZooKeeperConnectionTimeout = time.Duration(timeoutSeconds) * time.Second
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

// getOneConfig returns one configuration item from the scanner.
func getOneConfig(scanner *bufio.Scanner) (string, string, error) {
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue // Skip comment lines and empty lines
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue // Skip lines without key=value format
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		return key, value, nil
	}
	if err := scanner.Err(); err != nil {
		return "", "", err
	}
	return "", "", nil
}
