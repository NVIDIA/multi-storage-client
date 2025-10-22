package main

import (
	"fmt"
	"os"
	"testing"
)

type testGlobalsStruct struct {
	testMountPoint        string
	testConfigFilePathMap map[string]string
}

var testGlobals testGlobalsStruct

// `TestMain` provides the wrapper around all of the test cases to establish resources common to all.
func TestMain(m *testing.M) {
	var (
		err                      error
		runExitCode              int
		testConfigFile           *os.File
		testConfigFilePath       string
		testConfigFilePathSuffix string
	)

	testGlobals.testMountPoint, err = os.MkdirTemp("", "MSCPosixTestMountPoint*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.MkdirTemp(\"\", \"MSCPosixTestMountPoint*\") failed: %v\n", err)
		os.Exit(1)
	}

	testGlobals.testConfigFilePathMap = make(map[string]string)

	for _, testConfigFilePathSuffix = range []string{".json", ".yaml", ".yml", ".other", ""} {
		testConfigFile, err = os.CreateTemp("", "MSCPosixTestConfigFile*"+testConfigFilePathSuffix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "os.CreateTemp(\"\", \"MSCPosixTestConfigFile*%s\") failed: %v\n", testConfigFilePathSuffix, err)
			os.Exit(1)
		}

		testGlobals.testConfigFilePathMap[testConfigFilePathSuffix] = testConfigFile.Name()

		err = testConfigFile.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "tempConfigFile.Close() failed: %v", err)
			os.Exit(1)
		}
	}

	runExitCode = m.Run()

	for _, testConfigFilePath = range testGlobals.testConfigFilePathMap {
		err = os.Remove(testConfigFilePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "os.Remove(testConfigFilePath) failed: %v", err)
			os.Exit(1)
		}
	}

	err = os.Remove(testGlobals.testMountPoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "os.Remove(testGlobals.testMountPoint) failed: %v", err)
		os.Exit(1)
	}

	os.Exit(runExitCode)
}

// `testOsArgs` constructs a string slice containing the supplied configFilePath to pass to initGlobals().
func testOsArgs(configFilePath string) (osArgs []string) {
	osArgs = []string{os.Args[0], configFilePath}
	return
}
