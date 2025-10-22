package main

import (
	"fmt"
	"testing"
)

// [TODO] fission_test.go needs to test root dir, backend dir, subdir, and files for these fission callbacks:
//
//   DoLookup
//   DoGetAttr
//   DoOpen
//   DoRead
//   DoStatFS
//   DoRelease
//   DoInit
//   DoOpenDir
//   DoReadDir
//   DoReleaseDir
//   DoReadDirPlus
//   DoStatX

const testFissionConfAsString = `
mscp_version: 1
backends: [
  {
    dir_name: test,
	bucket_container_name: ignored,
	backend_type: RAM,
  },
]`

func HIDETestFission(t *testing.T) {
	fmt.Printf("testFissionConfAsString: \"%s\"\n", testFissionConfAsString)
	fmt.Printf("testGlobals: %#v\n", testGlobals)
}
