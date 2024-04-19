package testutil

import (
	"fmt"
	"os"
	"time"
)

// TestZKPrinter represents the type used for ZK to log.
type TestZKPrinter struct {
	Out *os.File
}

// Printf is the necessary method for ZK to log.
func (p *TestZKPrinter) Printf(format string, a ...interface{}) {
	timePrefix := time.Now().Format("2006-01-02 15:04:05 ")
	_, _ = p.Out.WriteString(timePrefix + fmt.Sprintf(format, a...) + "\n")
}
