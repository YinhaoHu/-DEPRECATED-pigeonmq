package util

import (
	"bufio"
	"strings"
)

// GetOneConfig returns one configuration item from the scanner.
func GetOneConfig(scanner *bufio.Scanner) (string, string, error) {
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
