package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// `main` is the entrypoint for the FUSE file system daemon. It parses the
// command line. Help text will be output if explicitly requested or the
// command line arguments are not understood. In other cases, it requires
// a successful parsing of the configuration file whose location is
// determined in the initGlobals() call. Next, the FUSE file system is
// initialized and the configuration file specified backends are mounted
// beneath the root of the FUSE file system. The daemon then enters a loop
// until receiving a SIGINT or SIGTERM. Either periodically or in response
// to a SIGHUP, the configuration file is re-read and the list of backends
// is adjusted based on any changes detected.
func main() {
	var (
		displayHelp         bool
		displayHelpMatchSet map[string]struct{}
		err                 error
		signalChan          chan os.Signal
		signalReceived      os.Signal
		ticker              *time.Ticker
	)

	displayHelpMatchSet = make(map[string]struct{})
	displayHelpMatchSet["-?"] = struct{}{}
	displayHelpMatchSet["-h"] = struct{}{}
	displayHelpMatchSet["help"] = struct{}{}
	displayHelpMatchSet["-help"] = struct{}{}
	displayHelpMatchSet["--help"] = struct{}{}
	displayHelpMatchSet["-v"] = struct{}{}
	displayHelpMatchSet["-version"] = struct{}{}
	displayHelpMatchSet["--version"] = struct{}{}

	switch len(os.Args) {
	case 1:
		displayHelp = false
	case 2:
		_, displayHelp = displayHelpMatchSet[os.Args[1]]
	default:
		displayHelp = true
	}

	if displayHelp {
		fmt.Printf("usage: %s [{-?|-h|help|-help|--help|-v|-version|--version} | <config-file>]\n", os.Args[0])
		fmt.Printf("  where a <config-file>, ending in suffix .yaml, .yml, or .json is to be found while searching:\n")
		fmt.Printf("    ${MSC_CONFIG}\n")
		fmt.Printf("    ${XDG_CONFIG_HOME}/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    ${HOME}/.msc_config.{yaml|yml|json}\n")
		fmt.Printf("    ${HOME}/.config/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    ${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    /etc/msc_config.{yaml|yml|json}\n")
		fmt.Printf("version:\n")
		fmt.Printf("  %s\n", GitTag)
		os.Exit(0)
	}

	initGlobals()

	err = checkConfigFile()
	if err != nil {
		globals.logger.Fatalf("parsing config-file (\"%s\") failed: %v", globals.configFilePath, err)
	}

	initFS()

	processToMountList()

	err = performFissionMount()
	if err != nil {
		globals.logger.Fatalf("unable to perform FUSE mount [Err: %v]", err)
	}

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	if globals.config.autoSIGHUPInterval == 0 {
		ticker = time.NewTicker(365 * 24 * time.Hour)
		ticker.Stop()
	} else {
		ticker = time.NewTicker(globals.config.autoSIGHUPInterval)
	}

	for {
		select {
		case signalReceived = <-signalChan:
			if signalReceived != syscall.SIGHUP {
				// We received either syscall.SIGINT or syscall.SIGTERM...so terminate normally

				err = performFissionUnmount()
				if err != nil {
					globals.logger.Fatalf("unexpected error during FUSE unmount: %v", err)
				}

				drainFS()

				os.Exit(0)
			}

			// We received a syscall.SIGHUP... so re-parse (current) content of globals.condfigFilePath and resume

			err = checkConfigFile()
			if err != nil {
				globals.logger.Printf("parsing config-file (\"%s\") failed: %v", globals.configFilePath, err)
			}

			processToUnmountList()

			processToMountList()
		case <-ticker.C:
			err = checkConfigFile()
			if err != nil {
				globals.logger.Print(err)
			}
		case err = <-globals.errChan:
			// We received an Unexpected exit of /dev/fuse read loop... to terminate abnormally

			globals.logger.Fatalf("received unexpected FUSE error: %v", err)
		}
	}
}
