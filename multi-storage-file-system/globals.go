package main

import (
	"container/list"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/fission/v4"
	"github.com/cockroachdb/pebble/v2"
)

//go:generate go run ./tools/lockgen -dir .

var GitTag string // This variable will be populated at build time

const (
	MSFSVersionPythonCompatibility = uint64(0)
	MSFSVersionOne                 = uint64(1)
)

// `backendConfigAIStoreStruct` describes a backend's AIStore-specific settings.
// Note: AIStore SDK handles retries internally, so no retry config needed.
type backendConfigAIStoreStruct struct {
	// From <config-file>
	endpoint                 string        //      JSON/YAML "endpoint"                       default:"${AIS_ENDPOINT}"
	skipTLSCertificateVerify bool          //      JSON/YAML "skip_tls_certificate_verify"    default:false
	authnToken               string        //      JSON/YAML "authn_token"                    default:"${AIS_AUTHN_TOKEN}"
	authnTokenFile           string        //      JSON/YAML "authn_token_file"               default:"${AIS_AUTHN_TOKEN_FILE:=~/.config/ais/cli/auth.token}"
	provider                 string        //      JSON/YAML "provider"                       default:"s3"
	timeout                  time.Duration //      JSON/YAML "timeout"                        default:30000
}

// `backendConfigGCSStruct` describes a backend's GCS-specific settings.
type backendConfigGCSStruct struct {
	apiKey                   string        //      JSON/YAML "api_key"                        default:""
	endpoint                 string        //      JSON/YAML "endpoint"                       default:""
	skipTLSCertificateVerify bool          //      JSON/YAML "skip_tls_certificate_verify"    default:false
	retryBaseDelay           time.Duration //      JSON/YAML "retry_base_delay"               default:10
	retryNextDelayMultiplier float64       //      JSON/YAML "retry_next_delay_multiplier"    default:2.0
	retryMaxDelay            time.Duration //      JSON/YAML "retry_max_delay"                default:2000
}

// `backendConfigPSEUDOStruct` describes a backend's RAM-specific settings.
type backendConfigPSEUDOStruct struct {
	// From <config-file>
	dirNameFormat           string        //       JSON/YAML "dir_name_format"                default:"dir_%08X"
	fileNameFormat          string        //       JSON/YAML "file_name_format"               default:"file_%08X"
	dirStartingNumber       uint64        //       JSON/YAML "dir_starting_number"            default:0
	fileStartingNumber      uint64        //       JSON/YAML "file_starting_number"           default:0
	fileSize                uint64        //       JSON/YAML "file_size"                      default:0
	filesAtDepth0           uint64        //       JSON/YAML "files_at_depth_0"               default:0
	filesAtDepth1           uint64        //       JSON/YAML "files_at_depth_1"               default:0
	filesAtDepth2           uint64        //       JSON/YAML "files_at_depth_2"               default:0
	filesAtDepth3           uint64        //       JSON/YAML "files_at_depth_3"               default:0
	maxListPageSize         uint64        //       JSON/YAML "max_list_page_size"             default:1000
	minLatencyDeleteFile    time.Duration //       JSON/YAML "min_latency_delete_file"        default 0
	minLatencyListDirectory time.Duration //       JSON/YAML "min_latency_list_directory"     default 0
	minLatencyListObjects   time.Duration //       JSON/YAML "min_latency_list_objects"       default 0
	minLatencyReadFile      time.Duration //       JSON/YAML "min_latency_read_file"          default 0
	minLatencyStatDirectory time.Duration //       JSON/YAML "min_latency_stat_directory"     default 0
	minLatencyStatFile      time.Duration //       JSON/YAML "min_latency_stat_file"          default 0
	subdirectoriesAtDepth0  uint64        //       JSON/YAML "subdirectories_at_depth_0"      default:0
	subdirectoriesAtDepth1  uint64        //       JSON/YAML "subdirectories_at_depth_1"      default:0
	subdirectoriesAtDepth2  uint64        //       JSON/YAML "subdirectories_at_depth_2"      default:0
	// Runtime state
	maxPathDepth              uint64    //         Convenience recording of largest depth <N> where filesAtDepth<N> > 0 or == 0 if filesAtDepth<N> are all == 0
	filesAtDepth              [4]uint64 //         Convenience representation of .filesAtDepth[0-3]
	subdirectoriesAtDepth     [4]uint64 //         Convenience representation of .subdirectoriesAtDepth[0-2] with padding to match size of .filesAtDepth
	objectsInDirectoryAtDepth [4]uint64 //         Conenvience representation of the number of objects inside directory at depth 0-3
}

// `backendConfigRAMStruct` describes a backend's RAM-specific settings.
type backendConfigRAMStruct struct {
	// From <config-file>
	maxListPageSize     uint64 //                  JSON/YAML "max_list_page_size"             default:1000
	maxTotalObjectSpace uint64 //                  JSON/YAML "max_total_object_space"         default:1073741824
	maxTotalObjects     uint64 //                  JSON/YAML "max_total_objects"              default:10000
}

// `backendConfigS3Struct` describes a backend's S3-specific settings.
type backendConfigS3Struct struct {
	// From <config-file>
	configCredentialsProfile  string        //     JSON/YAML "config_credentials_profile"     default:"${AWS_PROFILE:-default}"
	useConfigEnv              bool          //     JSON/YAML "use_config_env"                 default:false
	configFilePath            string        //     JSON/YAML "config_file_path"               default:"${AWS_CONFIG_FILE:-~/.aws/config}"
	region                    string        //     JSON/YAML "region"                         default:"${AWS_REGION:-us-east-1}"
	endpoint                  string        //     JSON/YAML "endpoint"                       default:"${AWS_ENDPOINT}"
	useCredentialsEnv         bool          //     JSON/YAML "use_credentials_env"            default:false
	credentialsFilePath       string        //     JSON/YAML "credentials_file_path"          default:"${AWS_SHARED_CREDENTIALS_FILE:-~/.aws/credentials}"
	accessKeyID               string        //     JSON/YAML "access_key_id"                  default:"${AWS_ACCESS_KEY_ID}"
	secretAccessKey           string        //     JSON/YAML "secret_access_key"              default:"${AWS_SECRET_ACCESS_KEY}"
	skipTLSCertificateVerify  bool          //     JSON/YAML "skip_tls_certificate_verify"    default:false
	virtualHostedStyleRequest bool          //     JSON/YAML "virtual_hosted_style_request"   default:false
	unsignedPayload           bool          //     JSON/YAML "unsigned_payload"               default:false
	retryBaseDelay            time.Duration //     JSON/YAML "retry_base_delay"               default:10
	retryNextDelayMultiplier  float64       //     JSON/YAML "retry_next_delay_multiplier"    default:2.0
	retryMaxDelay             time.Duration //     JSON/YAML "retry_max_delay"                default:2000
	// Runtime state
	retryDelay []time.Duration //                  Delay slice indexed by RetryDelay()'s attempt arg - 1
}

// `backendStruct` contains the generic backend's settings and runtime
// particulars as well is references to backendType-specific details.
type backendStruct struct {
	// From <config-file>
	dirName                     string      //     JSON/YAML "dir_name"                       required
	readOnly                    bool        //     JSON/YAML "readonly"                       default:true
	flushOnClose                bool        //     JSON/YAML "flush_on_close"                 default:true
	uid                         uint64      //     JSON/YAML "uid"                            default:<current euid>
	gid                         uint64      //     JSON/YAML "gid"                            default:<current egid>
	dirPerm                     uint64      //     JSON/YAML "dir_perm"                       default:0o555(ro)/0o777(rw)
	filePerm                    uint64      //     JSON/YAML "file_perm"                      default:0o444(ro)/0o666(rw)
	directoryPageSize           uint64      //     JSON/YAML "directory_page_size"            default:0(endpoint determined)
	multiPartCacheLineThreshold uint64      //     JSON/YAML "multipart_cache_line_threshold" default:512
	uploadPartCacheLines        uint64      //     JSON/YAML "upload_part_cache_lines"        default:32
	uploadPartConcurrency       uint64      //     JSON/YAML "upload_part_concurrency"        default:32
	bucketContainerName         string      //     JSON/YAML "bucket_container_name"          required
	prefix                      string      //     JSON/YAML "prefix"                         default:""
	traceLevel                  uint64      //     JSON/YAML "trace_level"                    default:0
	backendType                 string      //     JSON/YAML "backend_type"                   required(one of "AIStore", "GCS", "PSEUDO", "RAM", "S3")
	backendTypeSpecifics        interface{} //                                                as-required(one of *backendConfig{AIStore|GCS|PSEUDO|RAM|S3}Struct)
	// Runtime state
	nonce          uint64                //        Key in globalsStruct.backendMap
	backendPath    string                //        URL incorporating each of the above path-related values
	context        backendContextIf      //
	inode          *inodeStruct          //        Link to this backendStruct's inodeStruct with .inodeType == BackendRootDir
	fissionMetrics *fissionMetricsStruct //
	backendMetrics *backendMetricsStruct //
	mounted        bool                  //        If false, backendStruct.dirName not in fuseRootDirInodeMAP
}

// `configStruct` describes the global configuration settings as well as the array of backendStruct's configured.
type configStruct struct {
	// From <config-file>
	msfsVersion                               uint64                     // JSON/YAML "msfs_version"                                      default:0
	mountName                                 string                     // JSON/YAML "mountname"                                         default:"msfs"
	mountPoint                                string                     // JSON/YAML "mountpoint"                                        default:"${MSFS_MOUNTPOINT:-/mnt}""
	fuseWorkers                               uint64                     // JSON/YAML "fuse_workers"                                      default:0
	fuseFdPerWorker                           bool                       // JSON/YAML "fuse_fd_per_worker"                                default:false
	uid                                       uint64                     // JSON/YAML "uid"                                               default:<current euid>
	gid                                       uint64                     // JSON/YAML "gid"                                               default:<current egid>
	dirPerm                                   uint64                     // JSON/YAML "dir_perm"                                          default:0o555
	allowOther                                bool                       // JSON/YAML "allow_other"                                       default:true
	maxWrite                                  uint64                     // JSON/YAML "max_write"                                         default:131072 (128Ki)
	entryAttrTTL                              time.Duration              // JSON/YAML "entry_attr_ttl"                                    default:10000 (in milliseconds)
	evictableInodeTTL                         time.Duration              // JSON/YAML "evictable_inode_ttl"                               default:1000000 (in milliseconds)
	virtualDirTTL                             time.Duration              // JSON/YAML "virtual_dir_ttl"                                   default:1000000 (in milliseconds)
	virtualFileTTL                            time.Duration              // JSON/YAML "virtual_file_ttl"                                  default:1000000 (in milliseconds)
	ttlCheckInterval                          time.Duration              // JSON/YAML "ttl_check_interval"                                default:250 (in milliseconds)
	cacheLineSize                             uint64                     // JSON/YAML "cache_line_size"                                   default:10485760 (10Mi)
	cacheLines                                uint64                     // JSON/YAML "cache_lines"                                       default:128
	cacheLinesToPrefetch                      uint64                     // JSON/YAML "cache_lines_to_prefetch"                           default:4
	dirtyCacheLinesFlushTrigger               uint64                     // JSON/YAML "dirty_cache_lines_flush_trigger"                   default:80 (as a percentage)
	dirtyCacheLinesMax                        uint64                     // JSON/YAML "dirty_cache_lines_max"                             default:90 (as a percentage)
	cacheDirPath                              string                     // JSON/YAML "cache_dir_path"                                    default:""
	metadataCachePagingMode                   string                     // JSON/YAML "metadata_cache_paging_mode"                        default:"pebble"
	pebbleCacheSize                           uint64                     // JSON/YAML "pebble_cache_size"                                 default:33554432 (32Mi)
	pebbleL0CompactionFileThreshold           uint64                     // JSON/YAML "pebble_l0_compaction_file_threshold"               default:4
	pebbleL0StopWritesThreshold               uint64                     // JSON/YAML "pebble_l0_stop_writes_threshold"                   default:12
	pebbleMemTableSize                        uint64                     // JSON/YAML "pebble_mem_table_size"                             default:8388608 (8Mi)
	inodeMapKeysPerPageMax                    uint64                     // JSON/YAML "inode_map_keys_per_page_max"                       default:400
	inodeMapPageEvictLowLimit                 uint64                     // JSON/YAML "inode_map_page_evict_low_limit"                    default:100
	inodeMapPageEvictHighLimit                uint64                     // JSON/YAML "inode_map_page_evict_high_limit"                   default:104
	inodeMapPageDirtyFlushTrigger             uint64                     // JSON/YAML "inode_map_page_dirty_flush_trigger"                default:50
	inodeMapFlushedPerGC                      uint64                     // JSON/YAML "inode_map_flushes_per_gc"                          default:10
	inodeEvictionQueueKeysPerPageMax          uint64                     // JSON/YAML "inode_eviction_queue_keys_per_page_max"            default:300
	inodeEvictionQueuePageEvictLowLimit       uint64                     // JSON/YAML "inode_eviction_queue_page_evict_low_limit"         default:100
	inodeEvictionQueuePageEvictHighLimit      uint64                     // JSON/YAML "inode_eviction_queue_page_evict_high_limit"        default:104
	inodeEvictionQueuePageDirtyFlushTrigger   uint64                     // JSON/YAML "inode_eviction_queue_page_dirty_flush_trigger"     default:50
	inodeEvictionQueueFlushedPerGC            uint64                     // JSON/YAML "inode_eviction_queue_flushes_per_gc"               default:10
	physChildDirEntryMapKeysPerPageMax        uint64                     // JSON/YAML "phys_child_dir_entry_map_keys_per_page_max"        default:250
	physChildDirEntryMapPageEvictLowLimit     uint64                     // JSON/YAML "phys_child_dir_entry_map_page_evict_low_limit"     default:100
	physChildDirEntryMapPageEvictHighLimit    uint64                     // JSON/YAML "phys_child_dir_entry_map_page_evict_high_limit"    default:104
	physChildDirEntryMapPageDirtyFlushTrigger uint64                     // JSON/YAML "phys_child_dir_entry_map_page_dirty_flush_trigger" default:50
	physChildDirEntryMapFlushedPerGC          uint64                     // JSON/YAML "phys_child_dir_entry_map_flushes_per_gc"           default:10
	virtChildDirEntryMapKeysPerPageMax        uint64                     // JSON/YAML "virt_child_dir_entry_map_keys_per_page_max"        default:250
	virtChildDirEntryMapPageEvictLowLimit     uint64                     // JSON/YAML "virt_child_dir_entry_map_page_evict_low_limit"     default:100
	virtChildDirEntryMapPageEvictHighLimit    uint64                     // JSON/YAML "virt_child_dir_entry_map_page_evict_high_limit"    default:104
	virtChildDirEntryMapPageDirtyFlushTrigger uint64                     // JSON/YAML "virt_child_dir_entry_map_page_dirty_flush_trigger" default:50
	virtChildDirEntryMapFlushedPerGC          uint64                     // JSON/YAML "virt_child_dir_entry_map_flushes_per_gc"           default:10
	processMemoryLimit                        uint64                     // JSON/YAML "process_memory_limit"                              default:4294967296 (4Gi)
	autoSIGHUPInterval                        time.Duration              // JSON/YAML "auto_sighup_interval"                              default:0 (none)
	observability                             *observabilityConfigStruct // JSON/YAML "observability"                                     default:nil (disabled)
	endpoint                                  string                     // JSON/YAML "endpoint"                                          default:""
	backends                                  map[string]*backendStruct  // JSON/YAML "backends"                                          Key == backendStruct.mountPointSubdirectoryName
}

// observabilityConfigStruct holds observability configuration
// Matches MSC Python schema exactly: opentelemetry.metrics.{attributes, reader, exporter}
type observabilityConfigStruct struct {
	// Metrics configuration (matches Python schema)
	metricsAttributes    []attributeProviderStruct // JSON/YAML "metrics.attributes"
	metricsReaderOptions *readerOptionsStruct      // JSON/YAML "metrics.reader.options"
	metricsExporter      *exporterStruct           // JSON/YAML "metrics.exporter"
}

// attributeProviderStruct matches Python's EXTENSION_SCHEMA for attributes
type attributeProviderStruct struct {
	Type    string                 // JSON/YAML "type"    e.g. "static", "host", "process", "msc_config"
	Options map[string]interface{} // JSON/YAML "options" type-specific options
}

// readerOptionsStruct matches Python's reader.options
type readerOptionsStruct struct {
	CollectIntervalMillis uint64 // JSON/YAML "collect_interval_millis" default:1000 (1 second)
	CollectTimeoutMillis  uint64 // JSON/YAML "collect_timeout_millis"  default:10000 (10 seconds)
	ExportIntervalMillis  uint64 // JSON/YAML "export_interval_millis"  default:60000 (60 seconds)
	ExportTimeoutMillis   uint64 // JSON/YAML "export_timeout_millis"   default:30000 (30 seconds)
}

// exporterStruct matches Python's EXTENSION_SCHEMA for exporter
type exporterStruct struct {
	Type    string                 // JSON/YAML "type"    e.g. "otlp", "console"
	Options map[string]interface{} // JSON/YAML "options" e.g. {"endpoint": "http://localhost:4318/v1/metrics"}
}

const (
	FUSERootDirInodeNumber uint64 = 1
)

const (
	DefaultMountPoint = "/mnt"
	EnvMSFSMountPoint = "MSFS_MOUNTPOINT"
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
	nonce uint64 // Key in inodeStruct.fhSet & globalsStruct.fhMap
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
	PhysChildInodeMap = "inodeStruct.physChildInodeMap"
	VirtChildInodeMap = "inodeStruct.virtChildInodeMap"

	InodeEvictionLRU = "globalsStruct.inodeEvictionLRU"

	PrefetchDirectoryList = "globals.Struct.prefetchDirectoryList"
)

const (
	CacheLineFree uint8 = iota
	CacheLineInbound
	CacheLineClean
	CacheLineOutbound
	CacheLineDirty
)

// `cacheLineStruct` contains both the state and content of a cache line used to hold file inode content.
type cacheLineStruct struct {
	nonce       uint64            // Key in globalsStruct.cacheMap
	listElement *list.Element     // Tracks position on state-corresponding globals.{inboundCacheLineList|cleanCacheLineLRU|outboundCacheLineList|dirtyCacheLineLRU}
	state       uint8             // One of CacheLine*; determines membership in one of globals.inboundCacheLineList, globals.cleanCacheLineLRU, globals.outboundCacheLineList, or globals.dirtyCacheLineLRU
	waiters     []*sync.WaitGroup // List of those awaiting a state change
	inodeNumber uint64            // Reference to an inodeStruct.inodeNumber
	lineNumber  uint64            // Identifies file/object range covered by content as up to [lineNumber * globals.config.cacheLineSize:(lineNumber + 1) * global.config.cacheLineSize)
	eTag        string            // If state == CacheLineClean, value of inodeStruct.eTag when when fetched from backend; Otherwise, == ""
	content     []byte            // File/Object content for the range (up to) [lineNumber * globals.config.cacheLineSize:(lineNumber + 1) * global.config.cacheLineSize)
}

// `dataCacheLineLinkStruct` is used to both provide a doubly linked list head as well as
// the links within a `dataCacheLineStateStruct` for
type dataCacheLineLinkStruct struct {
	next uint64
	prev uint64
}

// `dataCacheLineStateStruct` contains the state of each data cache line in globals.dataCacheLinesContent.
type dataCacheLineStateStruct struct {
	lru         dataCacheLineLinkStruct // Tracks position on one of globals.dataCacheLine{Free|Inbound|Clean|Output|Dirty}LRU
	state       uint8                   // One of CacheLine*; determines membership in one of globals.dataCacheLine{Free|Inbound|Clean|Output|Dirty}LRU
	contentLen  uint64                  // If <pos> is the position of this struct in globals.dataCacheLinesState, valid content is [:.contentLen] of globals.datdataCacheLinesContent[<pos>*globals.config.cacheLineSize:(<pos>+1)*globals.config.cacheLineSize]
	inodeNumber uint64                  // Reference to an inodeStruct.inodeNumber
	lineNumber  uint64                  // Identifies file/object range covered by content as up to [lineNumber * globals.config.cacheLineSize:(lineNumber + 1) * global.config.cacheLineSize)
	eTag        string                  // If state == CacheLineClean, value of inodeStruct.eTag when when fetched from backend; Otherwise, == ""
}

// `inodeStruct` contains the state of an inode.
//
// Note that this data structure is serialized and deserialized in bptree.go so changes here must be paired with changes there.
type inodeStruct struct {
	inodeNumber            uint64              // Note that, other than the FUSERootDir, any reference to a backend object path migtht change this value
	inodeType              uint32              // One of FileObject, FUSERootDir, BackendRootDir, or PseudoDir
	backendNonce           uint64              // If inodeType == FUSERootDir, == 0
	parentInodeNumber      uint64              // If inodeType == FUSERootDir, == .inodeNumber == FUSERootDirInodeNumber [Note: This is only a reference to a directory that may no longer be in globalsStruct.inodeMap]
	isVirt                 bool                // If == true, found on parent inodeStruct's .virtChild{Dir|File}Map; if == false, likely found on parent inodeStruct's .physChild{Dir|File}Map
	objectPath             string              // If inodeType == FUSERootDir, == ""; otherwise == path relative to backend.backendPath [inluding trailing slash if directory]
	basename               string              // If inodeType == FUSERootDir, == ""; otherwise == path/filepath.Base(.objectPath) [excluding trailing slash if directory]
	sizeInBackend          uint64              // If inodeType == FileObject, contains the size returned by the most recent backend call for it; otherwise == 0
	sizeInMemory           uint64              // If inodeType == FileObject, contains the size currently maintained in-memory only until the file is written to the backend; otherwise == 0
	eTag                   string              // If inodeType == FileObject, contains the eTag returned by the most recent call to readFileWrapper() for the object; otherwise == ""
	mode                   uint32              // If inodeType == FileObject, == (syscall.S_IFREG | file_perm); otherwise, == (syscall.S_IFDIR | dir_perm)
	mTime                  time.Time           // Time when this inodeStruct was last modified - note this is reported for aTime, bTime, and cTime as well
	xTime                  time.Time           // If != time.Time{}, marks the time when, if not recently accessed, the inode may be evicted
	isPrefetchInProgress   bool                // [inodeType == BackendRootDir || PseudoDir] indicates that a background prefetch of the directory is in progress
	cacheMap               map[uint64]uint64   // [inodeType == FileObject] Key == file offset / globals.config.cacheLineSize; Value = cacheLineStruct.nonce; &cacheLineStruct = globals.cacheMap[Value]
	inboundCacheLineCount  uint64              // [inodeType == FileObject] count of .cache[] elements in state CacheLineInbound
	outboundCacheLineCount uint64              // [inodeType == FileObject] count of .cache[] elements in state CacheLineOutbound
	dirtyCacheLineCount    uint64              // [inodeType == FileObject] count of .cache[] elements in state CacheLineDirty
	fhSet                  map[uint64]struct{} // Key == fhStruct.nonce; &fhStruct = globals.fhMap[Key]
	pendingDelete          bool                // [inodeType == FileObject] marked for deletion (prevents being reported in DoReadDir{|Plus}() output but also reuse until last file close enables removal)
}

// `globalsStruct` is the sync.Mutex protected global data structure under which all details about daemon state are tracked.
type globalsStruct struct {
	sync.Mutex                                                                         //
	logger                     *log.Logger                                             //
	metrics                    interface{}                                             // observability.MSFSMetrics (nil if observability disabled)
	meterProvider              interface{}                                             // *sdkmetric.MeterProvider (nil if observability disabled)
	configFilePath             string                                                  //
	config                     *configStruct                                           //
	configFileMap              map[string]interface{}                                  // Parsed config map for msc_config attribute provider
	backendsToUnmount          map[string]*backendStruct                               //
	backendsToMount            map[string]*backendStruct                               //
	backendsSkipped            map[string]struct{}                                     //
	backendMap                 map[uint64]*backendStruct                               // Key == backend.nonce
	errChan                    chan error                                              //
	fissionVolume              fission.Volume                                          //
	lastNonce                  uint64                                                  // Used to safely allocate non-repeating values (initialized to FUSERootDirInodeNumber to ensure skipping it)
	cacheDir                   string                                                  //
	pebbleDB                   *pebble.DB                                              // If .config.metadataCachePagingMode == "pebble", the handle to the PebbleDB; otherwise == nil
	inodeMap                   *inodeNumberToInodeStructMapStruct                      // Key: inodeStruct.inodeNumber;                                              Value: *inodeStruct
	inodeEvictionQueue         *xTimeInodeNumberSetStruct                              // Key: tuple(inodeStruct.xTime,inodeStruct.inodeNumber);                     Value: struct{}
	physChildDirEntryMap       *parentInodeNumberChildBasenameToChildInodeNumberStruct // Key: tuple(parent's inodeStruct.inodeNumber,child's inodeStruct.basename); Value: child's inodeStruct.inodeNumber
	virtChildDirEntryMap       *parentInodeNumberChildBasenameToChildInodeNumberStruct // Key: tuple(parent's inodeStruct.inodeNumber,child's inodeStruct.basename); Value: child's inodeStruct.inodeNumber
	inodeEvictorContext        context.Context                                         //
	inodeEvictorCancelFunc     context.CancelFunc                                      //
	inodeEvictorWaitGroup      sync.WaitGroup                                          //
	inboundCacheLineList       *list.List                                              // List of cacheLineStruct's where state == CacheLineInbound
	cleanCacheLineLRU          *list.List                                              // Contains cacheLineStruct.listElement's for state == CacheLineClean
	outboundCacheLineList      *list.List                                              // List of cacheLineStruct's where state == CacheLineOutbound
	dirtyCacheLineLRU          *list.List                                              // Contains cacheLineStruct.listElement's for state == CacheLineDirty
	cacheMap                   map[uint64]*cacheLineStruct                             // Key == cacheLineStruct.nonce
	dataCacheLinesFile         *os.File                                                // Mem-map'd file exposed via .dataCacheLinesContent
	dataCacheLinesContent      []byte                                                  // Holds the content of each data cache line who's state is at the equivalent position in .dataCacheLinesState
	dataCacheLinesState        []dataCacheLineStateStruct                              // Holds the state of each data cache line who's content is at the equivalent position in .dataCacheLinesContent
	dataCacheLineFreeLRU       dataCacheLineLinkStruct                                 // LRU-ordered doubly linked list of dataCacheLineStateStruct where .state == CacheLineFree
	dataCacheLineInboundLRU    dataCacheLineLinkStruct                                 // LRU-ordered doubly linked list of dataCacheLineStateStruct where .state == CacheLineInbound
	dataCacheLineCleanLRU      dataCacheLineLinkStruct                                 // LRU-ordered doubly linked list of dataCacheLineStateStruct where .state == CacheLineClean
	dataCacheLineOutboundLRU   dataCacheLineLinkStruct                                 // LRU-ordered doubly linked list of dataCacheLineStateStruct where .state == CacheLineOutbound
	dataCacheLineDirtyLRU      dataCacheLineLinkStruct                                 // LRU-ordered doubly linked list of dataCacheLineStateStruct where .state == CacheLineDirty
	dataCacheLineFreeCount     uint64                                                  // Count of elements on .dataCacheLineFreeLRU
	dataCacheLineInboundCount  uint64                                                  // Count of elements on .dataCacheLineInboundLRU
	dataCacheLineCleanCount    uint64                                                  // Count of elements on .dataCacheLineCleanLRU
	dataCacheLineOutboundCount uint64                                                  // Count of elements on .dataCacheLineOutboundLRU
	dataCacheLineDirtyCount    uint64                                                  // Count of elements on .dataCacheLineDirtyLRU
	fhMap                      map[uint64]*fhStruct                                    // Key == fhStruct.nonce
	fissionMetrics             *fissionMetricsStruct                                   //
	backendMetrics             *backendMetricsStruct                                   //
}

var globals globalsStruct

// `initGlobals` initializes the globalsStruct and locates the configuration file's path.
func initGlobals(osArgs []string) {
	var (
		homeEnv                         = os.Getenv("HOME")
		mscConfigEnv                    = os.Getenv("MSC_CONFIG")
		xdgConfigDir                    string
		xdgConfigDirContainedConfigFile bool
		xdgConfigDirsEnv                = os.Getenv("XDG_CONFIG_DIRS")
		xdgConfigHomeEnv                = os.Getenv("XDG_CONFIG_HOME")
	)

	globals.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmsgprefix) // |log.Lmicroseconds|log.Lshortfile

	globals.logger.Printf("[INFO] starting %s version %s", osArgs[0], GitTag)

	globals.backendsSkipped = make(map[string]struct{})

	for {
		if len(osArgs) == 2 {
			if !checkForFile(osArgs[1]) {
				dumpStack()
				globals.logger.Fatalf("[FATAL] file not readable at \"%s\"", osArgs[1])
			}
			globals.configFilePath = osArgs[1]
			break
		}

		if mscConfigEnv != "" {
			if !checkForFile(mscConfigEnv) {
				dumpStack()
				globals.logger.Fatalf("[FATAL] file not readable at non-empty ${MSC_CONFIG} [\"%s\"]", mscConfigEnv)
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

		dumpStack()
		globals.logger.Fatalf("[FATAL] config-file not found along search path")
	}

	globals.logger.Printf("[INFO] config-file path: \"%s\"", globals.configFilePath)

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

// `fetchNonce` is called while globals.Lock is held (via globalsLock) to grep the next
// `number only used once` value. The presumption here is that a
// simple incrementing uint64 would take many centuries to wrap
// around to zero that returned values from this func will never
// replicate earlier returned values.
func fetchNonce() (nonce uint64) {
	nonce = globals.lastNonce + 1
	globals.lastNonce = nonce

	return
}
