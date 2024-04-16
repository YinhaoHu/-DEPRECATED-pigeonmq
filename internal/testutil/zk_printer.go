package testutil

import (
	"fmt"
	"os"
	"time"
)

type TestZkPrinter struct {
	Out *os.File
}

func (p *TestZkPrinter) Printf(format string, a ...interface{}) {
	timePrefix := time.Now().Format("2006-01-02 15:04:05 ")
	_, _ = p.Out.WriteString(timePrefix + fmt.Sprintf(format, a...) + "\n")
}
