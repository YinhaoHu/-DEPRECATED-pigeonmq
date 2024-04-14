package util

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogLevel represents different log levels.
type LogLevel int

const (
	// DebugLevel indicates debug log level.
	DebugLevel LogLevel = iota
	// InfoLevel indicates info log level.
	InfoLevel
	// WarnLevel indicates warn log level.
	WarnLevel
	// ErrorLevel indicates error log level.
	ErrorLevel
)

// internalLogger represents a Logger interface.
type internalLogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// NewLogger creates a new Logger instance.
func NewLogger(out io.Writer, level LogLevel) *Logger {
	return &Logger{out: out, level: level, mu: sync.Mutex{}}
}

// Logger represents a concurrency safe log structure.
type Logger struct {
	out   io.Writer
	level LogLevel
	mu    sync.Mutex
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.level <= DebugLevel {
		l.log("DEBUG", format, args...)
	}
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level <= InfoLevel {
		l.log("INFO", format, args...)
	}
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	if l.level <= WarnLevel {
		l.log("WARN", format, args...)
	}
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.level <= ErrorLevel {
		l.log("ERROR", format, args...)
	}
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log("FATAL", format, args...)
	os.Exit(1)
}

func (l *Logger) log(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := fmt.Sprintf("[%s] "+format+"\n", append([]interface{}{level}, args...)...)
	_, _ = fmt.Fprintf(l.out, "%v %v", time.Now().Format("2006-01-02 15:04:05"), s)
}
