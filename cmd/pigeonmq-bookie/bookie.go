package main

import (
	"flag"
	"fmt"
	"os"
	"pigeonmq/internal/storage"
)

var configPath string

var bookie *storage.Bookie

func main() {
	handleCmdline()

	initBookie()
	bookie.Run()
}

// initBookie create the bookie object.
func initBookie() {
	cfg, err := storage.NewConfig(configPath)
	errCheckAndExit(err)

	bookie, err = storage.NewBookie(cfg)
	errCheckAndExit(err)
	bookie.Run()
}

// handleCmdline parse the command line arguments, check whether the command line is legal and fetch the
// necessary variables.
func handleCmdline() {
	var showHelp bool
	flag.BoolVar(&showHelp, "h", false, "Show help message")
	flag.BoolVar(&showHelp, "help", false, "Show help message")

	flag.StringVar(&configPath, "c", "", "Specify your config file path")
	flag.StringVar(&configPath, "config", "", "Specify your config file path")

	flag.Parse()

	if showHelp {
		showHelpPageAndExit()
	}
	if configPath == "" {
		fmt.Println("The configuration file should be specified.")
		os.Exit(1)
	}
}

// showHelpPageAndExit show the help page information and exit the process.
func showHelpPageAndExit() {
	const helpPage = `Usage: pigeon-bookie [OPTIONS]

Options:
  -h,--help	: Show this help message
  -c,--config <config file path> : Specify the config file path.
`

	fmt.Printf("%v", helpPage)
	os.Exit(1)
}

// errCheckAndExit if err is not nil, output the error information to the stderr and exit the program.
func errCheckAndExit(err error) {
	if err == nil {
		return
	}
	_, internalErr := fmt.Fprintf(os.Stderr, "[Initialization error] %v\n", err)
	if internalErr != nil {
		fmt.Println("initialization error: sending data to stderr.")
	}
	os.Exit(1)
}
