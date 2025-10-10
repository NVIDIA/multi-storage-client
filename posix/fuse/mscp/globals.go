package main

import (
	"container/list"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/fission/v3"
)

var GitTag string // This variable will be populated at build time

const (
	MSCPVersionPythonCompatibility = uint64(0)
	MSCPVersionOne                 = uint64(1)
)

type backendConfigAzureStruct struct{} // not currently supported
type backendConfigGCPStruct struct{}   // not currently supported
type backendConfigOCIStruct struct{}   // not currently supported

// `backendConfigS3Struct` describes a backend's S3-specific settings.
type backendConfigS3Struct struct {
	// From <config-file>
	accessKeyID               string        // JSON/YAML "access_key_id"                required
	secretAccessKey           string        // JSON/YAML "secret_access_key"            required
	region                    string        // JSON/YAML "region"                       required
	endpoint                  string        // JSON/YAML "endpoint"                     required
	allowHTTP                 bool          // JSON/YAML "allow_http"                   default:false
	skipTLSCertificateVerify  bool          // JSON/YAML "skip_tls_certificate_verify"  default:true
	virtualHostedStyleRequest bool          // JSON/YAML "virtual_hosted_style_request" default:false
	unsignedPayload           bool          // JSON/YAML "unsigned_payload"             default:false
	retryBaseDelay            time.Duration // JSON/YAML "retry_base_delay"             default:10
	retryNextDelayMultiplier  float64       // JSON/YAML "retry_next_delay_multiplier"  default:2.0
	retryMaxDelay             time.Duration // JSON/YAML "retry_max_delay"              default:10000
	// Runtime state
	retryDelay []time.Duration //              Delay slice indexed by RetryDelay()'s attempt arg - 1
}

// `backendStruct` contains the generic backend's settings and runtime
// particulars as well is references to backendType-specific details.
type backendStruct struct {
	// From <config-file>
	dirName                     string      // JSON/YAML "dir_name"                       required
	readOnly                    bool        // JSON/YAML "readonly"                       default:true
	flushOnClose                bool        // JSON/YAML "flush_on_close"                 default:true
	uid                         uint64      // JSON/YAML "uid"                            default:<current euid>
	gid                         uint64      // JSON/YAML "gid"                            default:<current egid>
	dirPerm                     uint64      // JSON/YAML "dir_perm"                       default:0o555(ro)/0o777(rw)
	filePerm                    uint64      // JSON/YAML "file_perm"                      default:0o444(ro)/0o666(rw)
	directoryPageSize           uint64      // JSON/YAML "directory_page_size"            default:0(endpoint determined)
	multiPartCacheLineThreshold uint64      // JSON/YAML "multipart_cache_line_threshold" default:512
	uploadPartCacheLines        uint64      // JSON/YAML "upload_part_cache_lines"        default:32
	uploadPartConcurrency       uint64      // JSON/YAML "upload_part_concurrency"        default:32
	bucketContainerName         string      // JSON/YAML "bucket_container_name"          required
	prefix                      string      // JSON/YAML "prefix"                         default:""
	traceLevel                  uint64      // JSON/YAML "trace_level"                    default:0
	backendType                 string      // JSON/YAML "backend_type"                   required(one of "Azure", "GCP", "OCI", "S3")
	backendTypeSpecifics        interface{} //                                            required(one of *backendConfig{Azure|GCP|OCI|S3}Struct)
	// Runtime state
	backendPath string                  //     URL incorporating each of the above path-related values
	context     backendContextIf        //
	inode       *inodeStruct            //     Link to this backendStruct's inodeStruct with .inodeType == BackendRootDir
	inodeMap    map[string]*inodeStruct //     Key: inodeStruct.objectPath
	mounted     bool                    //     If false, backendStruct.dirName not in fuseRootDirInodeMAP
}

// `configStruct` describes the global configuration settings as well as the array of backendStruct's configured.
type configStruct struct {
	mscpVersion                 uint64                    // JSON/YAML "mscp_version"                    default:0
	mountName                   string                    // JSON/YAML "mountname"                       default:"msc-posix"
	mountPoint                  string                    // JSON/YAML "mountpoint"                      default:"/mnt"
	uid                         uint64                    // JSON/YAML "uid"                             default:<current euid>
	gid                         uint64                    // JSON/YAML "gid"                             default:<current egid>
	dirPerm                     uint64                    // JSON/YAML "dir_perm"                        default:0o555
	allowOther                  bool                      // JSON/YAML "allow_other"                     default:true
	maxWrite                    uint64                    // JSON/YAML "max_write"                       default:131072(128Ki)
	entryAttrTTL                time.Duration             // JSON/YAML "entry_attr_ttl"                  default:1000(in milliseconds)
	evictableInodeTTL           time.Duration             // JSON/YAML "evictable_inode_ttl"             default:2000(in milliseconds)
	cacheLineSize               uint64                    // JSON/YAML "cache_line_size"                 default:1048576(1Mi)
	cacheLines                  uint64                    // JSON/YAML "cache_lines"                     default:4096
	dirtyCacheLinesFlushTrigger uint64                    // JSON/YAML "dirty_cache_lines_flush_trigger" default:80(as a percentage)
	dirtyCacheLinesMax          uint64                    // JSON/YAML "dirty_cache_lines_max"           default:90(as a percentage)
	autoSIGHUPInterval          time.Duration             // JSON/YAML "auto_sighup_interval"            default:0(none)
	backends                    map[string]*backendStruct // JSON/YAML "backends"                        Key == backendStruct.mountPointSubdirectoryName
}

const (
	FUSERootDirInodeNumber uint64 = 1
)

const (
	DotDirEntryBasename    = "."
	DotDotDirEntryBasename = ".."
)

const (
	FileObject     uint32 = iota // Transient inode populated by DoLookup(), DoReadDir(), and DoReadDirPlus() mapping to an object in a backend
	FUSERootDir                  // The "root" of the FUSE file system (i.e. inodeNumber == 1)
	BackendRootDir               // Semi-permanent inode corresponding to the "root" of a particular backend
	PseudoDir                    // Transient inode populated by DoLookup(), DoReadDir(), and DoReadDirPlus() mapping to an object path (ending in "/") in a backend
)

const (
	FileReadOnly    uint32 = iota // DoWrite() not allowed
	FileWriteNormal               // DoWrite() allowed - will honor fission.WriteIn.Offset
	FileWriteAppend               // DoWrite() allowed - will ignore fission.WriteIn.Offset and simply append
)

const (
	NewChildDirEntOffsetMask = uint64(1) << 63
)

// `fhStruct` contains the state of a file handle for an inode.
type fhStruct struct {
	nonce uint64
	inode *inodeStruct
	// The following only applicable if inode.inodeType == FileObject
	isExclusive  bool
	allowReads   bool
	allowWrites  bool
	appendWrites bool // Only applicable if allowWrites == true
	// The following only applicable if inode.inodeType == BackendRootDir or PseudoDir after enumerating each dir_entry by walking .inode.childDirMap then .inode.childFileMap
	listDirectoryInProgress               bool
	listDirectorySequenceDone             bool
	prevListDirectoryOutput               *listDirectoryOutputStruct
	prevListDirectoryOutputFileLen        uint64
	prevListDirectoryOutputStartingOffset uint64
	nextListDirectoryOutput               *listDirectoryOutputStruct
	nextListDirectoryOutputFileLen        uint64
	nextListDirectoryOutputStartingOffset uint64
	listDirectorySubdirectorySet          map[string]struct{}
	listDirectorySubdirectoryList         []string
	// For inode.inodeType == FUSERootDir, enumerating each dir_entry by walking .inode.childDirMap then .inode.childFileMap
}

const (
	VirtChildDirMap  = "inodeStruct.virtChildDirMap"
	VirtChildFileMap = "inodeStruct.virtChildFileMap"
)

const (
	CacheLineInbound uint8 = iota
	CacheLineClean
	CacheLineOutbound
	CacheLineDirty
)

// `cacheLineStruct` contains both the stat and content of a cache line used to hold file inode content.
type cacheLineStruct struct {
	sync.WaitGroup               // Waiters should not block while holding globals.Lock
	listElement    *list.Element // If state == CacheLineClean, link into globals.cleanCacheLineLRU; if state == CacheLineDirty, link into globals.dirtyCacheLineLRU; otherwise == nil
	state          uint8         // One of CacheLine*; determines membership in one of globals.inboundCacheLineCount, globals.cleanCacheLineLRU, globals.outboundCacheLineCount, or globals.dirtyCacheLineLRU
	inodeNumber    uint64        // Reference to an inodeStruct.inodeNumber
	lineNumber     uint64        // Identifies file/object range covered by content as up to [lineNumber * globals.config.cacheLineSize:(lineNumber + 1) * global.config.cacheLineSize)
	eTag           string        // If state == CacheLineClean, value of inodeStruct.eTag when when fetched from backend; Otherwise, == ""
	content        []byte        // File/Object content for the range (up to) [lineNumber * globals.config.cacheLineSize:(lineNumber + 1) * global.config.cacheLineSize)
}

// `inodeStruct` contains the state of an inode.
type inodeStruct struct {
	inodeNumber       uint64                      // Note that, other than the FUSERootDir, any reference to a backend object path migtht change this value
	inodeType         uint32                      // One of FileObject, FUSERootDir, BackendRootDir, or PseudoDir
	backend           *backendStruct              // If inodeType == FUSERootDir, == nil
	parentInodeNumber uint64                      // If inodeType == FUSERootDir, == .inodeNumber == FUSERootDirInodeNumber [Note: This is only a reference to a directory that may no longer be in globalsStruct.inodeMap]
	isVirt            bool                        // If == true, found on parent inodeStruct's .virtChild{Dir|File}Map; if == false, likely found on parent inodeStruct's .physChild{Dir|File}Map
	objectPath        string                      // If inodeType == FUSERootDir, == ""; otherwise == path relative to backend.backendPath [inluding trailing slash if directory]
	basename          string                      // If inodeType == FUSERootDir, == ""; otherwise == path/filepath.Base(.objectPath) [excluding trailing slash if directory]
	sizeInBackend     uint64                      // If inodeType == FileObject, contains the size returned by the most recent backend call for it; otherwise == 0
	sizeInMemory      uint64                      // If inodeType == FileObject, contains the size currently maintained in-memory only until the file is written to the backend; otherwise == 0
	eTag              string                      // If inodeType == FileObject, contains the eTag returned by the most recent call to backend.context.readFile() for the object; otherwise == ""
	mode              uint32                      // If inodeType == FileObject, == (syscall.S_IFREG | file_perm); otherwise, == (syscall.S_IFDIR | dir_perm)
	mTime             time.Time                   // Time when this inodeStruct was last modified - note this is reported for aTime, bTime, and cTime as well
	lTime             time.Time                   // Time when this inodeStruct was last looked up - used, along with .listElement, to cache evict from globals.inodeMap
	listElement       *list.Element               // If .isEvictable() == true, link into globals.inodeLRU ordered by .lTime; otherwise == nil
	fhMap             map[uint64]*fhStruct        // Key == fhStruct.nonce; Value == *fhStruct
	virtChildDirMap   *stringToUint64MapStruct    // [inodeType != FileObject] maps ".", "..", backendStruct.dirName (== backendStruct.inode.basename), or (currently empty) PsuedoDir's inodeStruct.basename to its inodeStruct.inodeNumebr
	virtChildFileMap  *stringToUint64MapStruct    // [inodeType != FileObject] maps (not yet instantiated in backend) FileObject's inodeStruct.basename to its inodeStruct; will be empty if .inodeType == FUSERootDir
	cache             map[uint64]*cacheLineStruct // [inodeType == FileObject] Key == file offset / globals.config.cacheLineSize
}

// `globalsStruct` is the sync.Mutex protected global data structure under which all details about daemon state are tracked.
type globalsStruct struct {
	sync.Mutex                                       //
	logger                 *log.Logger               //
	configFilePath         string                    //
	config                 *configStruct             //
	backendsToUnmount      map[string]*backendStruct //
	backendsToMount        map[string]*backendStruct //
	backendsSkipped        map[string]struct{}       //
	errChan                chan error                //
	fissionVolume          fission.Volume            //
	lastNonce              uint64                    // Used to safely allocate non-repeating values (initialized to FUSERootDirInodeNumber to ensure skipping it)
	inode                  *inodeStruct              // Link to the lone inodeStruct with .inodeNumber == FUSERootDirInodeNumber && .inodeType == FUSERootDir
	inodeMap               map[uint64]*inodeStruct   // Key: inodeStruct.inodeNumber
	inodeLRU               *list.List                // Contains inodeStruct.listElement's that are evictable ordered by inodeStruct.lTime
	inodeEvictorContext    context.Context           //
	inodeEvictorCancelFunc context.CancelFunc        //
	inodeEvictorWaitGroup  sync.WaitGroup            //
	inboundCacheLineCount  uint64                    // Count of cacheLineStruct's where state == CacheLineInbound
	cleanCacheLineLRU      *list.List                // Contains cacheLineStruct.listElement's for state == CacheLineClean
	outboundCacheLineCount uint64                    // Count of cacheLineStruct's where state == CacheLineOutbound
	dirtyCacheLineLRU      *list.List                // Contains cacheLineStruct.listElement's for state == CacheLineDirty
}

var globals globalsStruct

// `initGlobals` initializes the globalsStruct and locates the configuration file's path.
func initGlobals() {
	var (
		homeEnv                         = os.Getenv("HOME")
		mscConfigEnv                    = os.Getenv("MSC_CONFIG")
		xdgConfigDir                    string
		xdgConfigDirContainedConfigFile bool
		xdgConfigDirsEnv                = os.Getenv("XDG_CONFIG_DIRS")
		xdgConfigHomeEnv                = os.Getenv("XDG_CONFIG_HOME")
	)

	globals.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime) // |log.Lmicroseconds|log.Lshortfile

	globals.logger.Printf("Starting %s version %s", os.Args[0], GitTag)

	globals.backendsSkipped = make(map[string]struct{})

	for {
		if len(os.Args) == 2 {
			if !checkForFile(os.Args[1]) {
				globals.logger.Fatalf("file not readable at \"%s\"", os.Args[1])
			}
			globals.configFilePath = os.Args[1]
			break
		}

		if mscConfigEnv != "" {
			if !checkForFile(mscConfigEnv) {
				globals.logger.Fatalf("file not readable at non-empty ${MSC_CONFIG} [\"%s\"]", mscConfigEnv)
			}
			globals.configFilePath = mscConfigEnv
			break
		}

		if xdgConfigHomeEnv != "" {
			if checkForFile(xdgConfigHomeEnv + "/msc/config.yaml") {
				globals.configFilePath = xdgConfigHomeEnv + "/msc/config.yaml"
				break
			}
			if checkForFile(xdgConfigHomeEnv + "/msc/config.yml") {
				globals.configFilePath = xdgConfigHomeEnv + "/msc/config.yml"
				break
			}
			if checkForFile(xdgConfigHomeEnv + "/msc/config.json") {
				globals.configFilePath = xdgConfigHomeEnv + "/msc/config.json"
				break
			}
		}

		if homeEnv != "" {
			if checkForFile(homeEnv + "/.msc_config.yaml") {
				globals.configFilePath = homeEnv + "/.msc_config.yaml"
				break
			}
			if checkForFile(homeEnv + "/.msc_config.yml") {
				globals.configFilePath = homeEnv + "/.msc_config.yml"
				break
			}
			if checkForFile(homeEnv + "/.msc_config.json") {
				globals.configFilePath = homeEnv + "/.msc_config.json"
				break
			}

			if checkForFile(homeEnv + "/.config/msc/config.yaml") {
				globals.configFilePath = homeEnv + "/.config/msc/config.yaml"
				break
			}
			if checkForFile(homeEnv + "/.config/msc/config.yml") {
				globals.configFilePath = homeEnv + "/.config/msc/config.yml"
				break
			}
			if checkForFile(homeEnv + "/.config/msc/config.json") {
				globals.configFilePath = homeEnv + "/.config/msc/config.json"
				break
			}
		}

		if xdgConfigDirsEnv == "" {
			if checkForFile("/etc/xdg/msc/config.yaml") {
				globals.configFilePath = "/etc/xdg/msc/config.yaml"
				break
			}
			if checkForFile("/etc/xdg/msc/config.yml") {
				globals.configFilePath = "/etc/xdg/msc/config.yml"
				break
			}
			if checkForFile("/etc/xdg/msc/config.json") {
				globals.configFilePath = "/etc/xdg/msc/config.json"
				break
			}
		} else { // xdgConfigDirsEnv != ""
			xdgConfigDirContainedConfigFile = false
			for _, xdgConfigDir = range strings.Split(xdgConfigDirsEnv, ":") {
				if checkForFile(xdgConfigDir + "/msc/config.yaml") {
					globals.configFilePath = xdgConfigDir + "/msc/config.yaml"
					xdgConfigDirContainedConfigFile = true
					break
				}
				if checkForFile(xdgConfigDir + "/msc/config.yml") {
					globals.configFilePath = xdgConfigDir + "/msc/config.yml"
					xdgConfigDirContainedConfigFile = true
					break
				}
				if checkForFile(xdgConfigDir + "/msc/config.json") {
					globals.configFilePath = xdgConfigDir + "/msc/config.json"
					xdgConfigDirContainedConfigFile = true
					break
				}
			}
			if xdgConfigDirContainedConfigFile {
				break
			}
		}

		if checkForFile("/etc/msc_config.yaml") {
			globals.configFilePath = "/etc/msc_config.yaml"
			break
		}
		if checkForFile("/etc/msc_config.yml") {
			globals.configFilePath = "/etc/msc_config.yml"
			break
		}
		if checkForFile("/etc/msc_config.json") {
			globals.configFilePath = "/etc/msc_config.json"
			break
		}

		globals.logger.Fatalf("config-file not found along search path")
	}

	globals.logger.Printf("config-file path: \"%s\"", globals.configFilePath)

	globals.config = nil
	globals.backendsToUnmount = make(map[string]*backendStruct)
	globals.backendsToMount = make(map[string]*backendStruct)

	globals.errChan = make(chan error, 1)
}

// `checkForFile` indicates whether or not a file exists at filePath.
func checkForFile(filePath string) (ok bool) {
	fileInfo, err := os.Stat(filePath)
	ok = (err == nil && !fileInfo.IsDir())
	return
}

// `fetchNonce` is called while globals.Lock is held to grep the next
// `number only used once` value. The presumption here is that a
// simple incrementing uint64 would take many centuries to wrap
// around to zero that returned values from this func will never
// replicate earlier returned values.
func fetchNonce() (nonce uint64) {
	nonce = globals.lastNonce + 1
	globals.lastNonce = nonce

	return
}
