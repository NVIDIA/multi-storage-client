package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

// `fissionMetricsStruct` is used to record metrics for the `fission` front end
// operations. Such metrics will be maintained globally as well as for both the
// top-level pseudo directory (holding the backend subdirectories) and for each
// of those backend subdirectory trees.
type fissionMetricsStruct struct {
	LookupSuccesses             prometheus.Counter
	LookupFailures              prometheus.Counter
	LookupSuccessLatencies      prometheus.Histogram
	LookupFailureLatencies      prometheus.Histogram
	GetAttrSuccesses            prometheus.Counter
	GetAttrFailures             prometheus.Counter
	GetAttrSuccessLatencies     prometheus.Histogram
	GetAttrFailureLatencies     prometheus.Histogram
	MkNodSuccesses              prometheus.Counter
	MkNodFailures               prometheus.Counter
	MkNodSuccessLatencies       prometheus.Histogram
	MkNodFailureLatencies       prometheus.Histogram
	MkDirSuccesses              prometheus.Counter
	MkDirFailures               prometheus.Counter
	MkDirSuccessLatencies       prometheus.Histogram
	MkDirFailureLatencies       prometheus.Histogram
	UnlinkSuccesses             prometheus.Counter
	UnlinkFailures              prometheus.Counter
	UnlinkSuccessLatencies      prometheus.Histogram
	UnlinkFailureLatencies      prometheus.Histogram
	RmDirSuccesses              prometheus.Counter
	RmDirFailures               prometheus.Counter
	RmDirSuccessLatencies       prometheus.Histogram
	RmDirFailureLatencies       prometheus.Histogram
	OpenSuccesses               prometheus.Counter
	OpenFailures                prometheus.Counter
	OpenSuccessLatencies        prometheus.Histogram
	OpenFailureLatencies        prometheus.Histogram
	ReadSuccesses               prometheus.Counter
	ReadFailures                prometheus.Counter
	ReadSuccessLatencies        prometheus.Histogram
	ReadFailureLatencies        prometheus.Histogram
	ReadSuccessSizes            prometheus.Histogram
	ReadFailureSizes            prometheus.Histogram
	ReadCacheHits               prometheus.Counter
	ReadCacheMisses             prometheus.Counter
	ReadCacheWaits              prometheus.Counter
	ReadCachePrefetches         prometheus.Counter
	StatFSCalls                 prometheus.Counter // Only applicable to globals.fissionMetrics
	ReleaseSuccesses            prometheus.Counter
	ReleaseFailures             prometheus.Counter
	ReleaseSuccessLatencies     prometheus.Histogram
	ReleaseFailureLatencies     prometheus.Histogram
	OpenDirSuccesses            prometheus.Counter
	OpenDirFailures             prometheus.Counter
	OpenDirSuccessLatencies     prometheus.Histogram
	OpenDirFailureLatencies     prometheus.Histogram
	ReadDirSuccesses            prometheus.Counter
	ReadDirFailures             prometheus.Counter
	ReadDirSuccessLatencies     prometheus.Histogram
	ReadDirFailureLatencies     prometheus.Histogram
	ReadDirEntriesReturned      prometheus.Counter
	ReleaseDirSuccesses         prometheus.Counter
	ReleaseDirFailures          prometheus.Counter
	ReleaseDirSuccessLatencies  prometheus.Histogram
	ReleaseDirFailureLatencies  prometheus.Histogram
	CreateSuccesses             prometheus.Counter
	CreateFailures              prometheus.Counter
	CreateSuccessLatencies      prometheus.Histogram
	CreateFailureLatencies      prometheus.Histogram
	ReadDirPlusSuccesses        prometheus.Counter
	ReadDirPlusFailures         prometheus.Counter
	ReadDirPlusSuccessLatencies prometheus.Histogram
	ReadDirPlusFailureLatencies prometheus.Histogram
	ReadDirPlusEntriesReturned  prometheus.Counter
	StatXSuccesses              prometheus.Counter
	StatXFailures               prometheus.Counter
	StatXSuccessLatencies       prometheus.Histogram
	StatXFailureLatencies       prometheus.Histogram
}

// `newFissionMetrics` provisions and initializes a `fissionMetricsStruct`.
func newFissionMetrics() (fissionMetrics *fissionMetricsStruct) {
	latencyBuckets := []float64{.000005, .000010, .000025, .000050, .000100, .000250, .000500, .001000, .002500, .005000, .010000, .025000, .050000, .100000, .250000, .500000, 1, 2.5, 5, 10}

	fissionMetrics = &fissionMetricsStruct{
		LookupSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_lookup_successes_total",
			Help: "Total number of successful Lookup operations",
		}),
		LookupFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_lookup_failures_total",
			Help: "Total number of failed Lookup operations",
		}),
		LookupSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_lookup_success_latency_seconds",
			Help:    "Latency of successful Lookup operations",
			Buckets: latencyBuckets,
		}),
		LookupFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_lookup_failure_latency_seconds",
			Help:    "Latency of failed Lookup operations",
			Buckets: latencyBuckets,
		}),

		GetAttrSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_getattr_successes_total",
			Help: "Total number of successful GetAttr operations",
		}),
		GetAttrFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_getattr_failures_total",
			Help: "Total number of failed GetAttr operations",
		}),
		GetAttrSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_getattr_success_latency_seconds",
			Help:    "Latency of successful GetAttr operations",
			Buckets: latencyBuckets,
		}),
		GetAttrFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_getattr_failure_latency_seconds",
			Help:    "Latency of failed GetAttr operations",
			Buckets: latencyBuckets,
		}),

		MkNodSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_mknod_successes_total",
			Help: "Total number of successful MkNod operations",
		}),
		MkNodFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_mknod_failures_total",
			Help: "Total number of failed MkNod operations",
		}),
		MkNodSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_mknod_success_latency_seconds",
			Help:    "Latency of successful MkNod operations",
			Buckets: latencyBuckets,
		}),
		MkNodFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_mknod_failure_latency_seconds",
			Help:    "Latency of failed MkNod operations",
			Buckets: latencyBuckets,
		}),

		MkDirSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_mkdir_successes_total",
			Help: "Total number of successful MkDir operations",
		}),
		MkDirFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_mkdir_failures_total",
			Help: "Total number of failed MkDir operations",
		}),
		MkDirSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_mkdir_success_latency_seconds",
			Help:    "Latency of successful MkDir operations",
			Buckets: latencyBuckets,
		}),
		MkDirFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_mkdir_failure_latency_seconds",
			Help:    "Latency of failed MkDir operations",
			Buckets: latencyBuckets,
		}),

		UnlinkSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_unlink_successes_total",
			Help: "Total number of successful Unlink operations",
		}),
		UnlinkFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_unlink_failures_total",
			Help: "Total number of failed Unlink operations",
		}),
		UnlinkSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_unlink_success_latency_seconds",
			Help:    "Latency of successful Unlink operations",
			Buckets: latencyBuckets,
		}),
		UnlinkFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_unlink_failure_latency_seconds",
			Help:    "Latency of failed Unlink operations",
			Buckets: latencyBuckets,
		}),

		RmDirSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_rmdir_successes_total",
			Help: "Total number of successful RmDir operations",
		}),
		RmDirFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_rmdir_failures_total",
			Help: "Total number of failed RmDir operations",
		}),
		RmDirSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_rmdir_success_latency_seconds",
			Help:    "Latency of successful RmDir operations",
			Buckets: latencyBuckets,
		}),
		RmDirFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_rmdir_failure_latency_seconds",
			Help:    "Latency of failed RmDir operations",
			Buckets: latencyBuckets,
		}),

		OpenSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_open_successes_total",
			Help: "Total number of successful Open operations",
		}),
		OpenFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_open_failures_total",
			Help: "Total number of failed Open operations",
		}),
		OpenSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_open_success_latency_seconds",
			Help:    "Latency of successful Open operations",
			Buckets: latencyBuckets,
		}),
		OpenFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_open_failure_latency_seconds",
			Help:    "Latency of failed Open operations",
			Buckets: latencyBuckets,
		}),

		ReadSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_successes_total",
			Help: "Total number of successful Read operations",
		}),
		ReadFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_failures_total",
			Help: "Total number of failed Read operations",
		}),
		ReadSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_read_success_latency_seconds",
			Help:    "Latency of successful Read operations",
			Buckets: latencyBuckets,
		}),
		ReadFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_read_failure_latency_seconds",
			Help:    "Latency of failed Read operations",
			Buckets: latencyBuckets,
		}),
		ReadSuccessSizes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_read_success_size_bytes",
			Help:    "Size of successful Read operations in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
		}),
		ReadFailureSizes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_read_failure_size_bytes",
			Help:    "Size of failed Read operations in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
		}),
		ReadCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_cache_hits_total",
			Help: "Total number of Read operation triggered cache hits",
		}),
		ReadCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_cache_misses_total",
			Help: "Total number of Read operation triggered cache misses",
		}),
		ReadCacheWaits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_cache_waits_total",
			Help: "Total number of Read operation triggered cache waits",
		}),
		ReadCachePrefetches: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_read_cache_prefetches_total",
			Help: "Total number of Read operation triggered cache prefetches",
		}),

		StatFSCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_statfs_calls_total",
			Help: "Total number of StatFS operations",
		}),

		ReleaseSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_release_successes_total",
			Help: "Total number of successful Release operations",
		}),
		ReleaseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_release_failures_total",
			Help: "Total number of failed Release operations",
		}),
		ReleaseSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_release_success_latency_seconds",
			Help:    "Latency of successful Release operations",
			Buckets: latencyBuckets,
		}),
		ReleaseFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_release_failure_latency_seconds",
			Help:    "Latency of failed Release operations",
			Buckets: latencyBuckets,
		}),

		OpenDirSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_opendir_successes_total",
			Help: "Total number of successful OpenDir operations",
		}),
		OpenDirFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_opendir_failures_total",
			Help: "Total number of failed OpenDir operations",
		}),
		OpenDirSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_opendir_success_latency_seconds",
			Help:    "Latency of successful OpenDir operations",
			Buckets: latencyBuckets,
		}),
		OpenDirFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_opendir_failure_latency_seconds",
			Help:    "Latency of failed OpenDir operations",
			Buckets: latencyBuckets,
		}),

		ReadDirSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdir_successes_total",
			Help: "Total number of successful ReadDir operations",
		}),
		ReadDirFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdir_failures_total",
			Help: "Total number of failed ReadDir operations",
		}),
		ReadDirSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_readdir_success_latency_seconds",
			Help:    "Latency of successful ReadDir operations",
			Buckets: latencyBuckets,
		}),
		ReadDirFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_readdir_failure_latency_seconds",
			Help:    "Latency of failed ReadDir operations",
			Buckets: latencyBuckets,
		}),
		ReadDirEntriesReturned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdir_entries_total",
			Help: "Total number of directory entries returned across successful ReadDir operations",
		}),

		ReleaseDirSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_releasedir_successes_total",
			Help: "Total number of successful ReleaseDir operations",
		}),
		ReleaseDirFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_releasedir_failures_total",
			Help: "Total number of failed ReleaseDir operations",
		}),
		ReleaseDirSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_releasedir_success_latency_seconds",
			Help:    "Latency of successful ReleaseDir operations",
			Buckets: latencyBuckets,
		}),
		ReleaseDirFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_releasedir_failure_latency_seconds",
			Help:    "Latency of failed ReleaseDir operations",
			Buckets: latencyBuckets,
		}),

		CreateSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_create_successes_total",
			Help: "Total number of successful Create operations",
		}),
		CreateFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_create_failures_total",
			Help: "Total number of failed Create operations",
		}),
		CreateSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_create_success_latency_seconds",
			Help:    "Latency of successful Create operations",
			Buckets: latencyBuckets,
		}),
		CreateFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_create_failure_latency_seconds",
			Help:    "Latency of failed Create operations",
			Buckets: latencyBuckets,
		}),

		ReadDirPlusSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdirplus_successes_total",
			Help: "Total number of successful ReadDirPlus operations",
		}),
		ReadDirPlusFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdirplus_failures_total",
			Help: "Total number of failed ReadDirPlus operations",
		}),
		ReadDirPlusSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_readdirplus_success_latency_seconds",
			Help:    "Latency of successful ReadDirPlus operations",
			Buckets: latencyBuckets,
		}),
		ReadDirPlusFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_readdirplus_failure_latency_seconds",
			Help:    "Latency of failed ReadDirPlus operations",
			Buckets: latencyBuckets,
		}),
		ReadDirPlusEntriesReturned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_readdirplus_entries_total",
			Help: "Total number of directory entries returned across successful ReadDirPlus operations",
		}),

		StatXSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_statx_successes_total",
			Help: "Total number of successful StatX operations",
		}),
		StatXFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fission_statx_failures_total",
			Help: "Total number of failed StatX operations",
		}),
		StatXSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_statx_success_latency_seconds",
			Help:    "Latency of successful StatX operations",
			Buckets: latencyBuckets,
		}),
		StatXFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fission_statx_failure_latency_seconds",
			Help:    "Latency of failed StatX operations",
			Buckets: latencyBuckets,
		}),
	}

	return
}

// `backendMetricsStruct` is used to record metrics for the `fission` front end
// operations. Such metrics will be maintained globally as well as for each backend.
type backendMetricsStruct struct {
	DeleteFileSuccesses           prometheus.Counter
	DeleteFileFailures            prometheus.Counter
	DeleteFileSuccessLatencies    prometheus.Histogram
	DeleteFileFailureLatencies    prometheus.Histogram
	ListDirectorySuccesses        prometheus.Counter
	ListDirectoryFailures         prometheus.Counter
	ListDirectorySuccessLatencies prometheus.Histogram
	ListDirectoryFailureLatencies prometheus.Histogram
	ListObjectsSuccesses          prometheus.Counter
	ListObjectsFailures           prometheus.Counter
	ListObjectsSuccessLatencies   prometheus.Histogram
	ListObjectsFailureLatencies   prometheus.Histogram
	ReadFileSuccesses             prometheus.Counter
	ReadFileFailures              prometheus.Counter
	ReadFileSuccessLatencies      prometheus.Histogram
	ReadFileFailureLatencies      prometheus.Histogram
	StatDirectorySuccesses        prometheus.Counter
	StatDirectoryFailures         prometheus.Counter
	StatDirectorySuccessLatencies prometheus.Histogram
	StatDirectoryFailureLatencies prometheus.Histogram
	StatFileSuccesses             prometheus.Counter
	StatFileFailures              prometheus.Counter
	StatFileSuccessLatencies      prometheus.Histogram
	StatFileFailureLatencies      prometheus.Histogram

	DirectoryPrefetchLatencies prometheus.Histogram
}

// `newBackendMetrics` provisions and initializes a `backendMetricsStruct`.
func newBackendMetrics() (backendMetrics *backendMetricsStruct) {
	latencyBuckets := prometheus.DefBuckets

	backendMetrics = &backendMetricsStruct{
		DeleteFileSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_delete_file_successes_total",
			Help: "Total number of successful DeleteFile operations",
		}),
		DeleteFileFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_delete_file_failures_total",
			Help: "Total number of failed DeleteFile operations",
		}),
		DeleteFileSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_delete_file_success_latency_seconds",
			Help:    "Latency of successful DeleteFile operations",
			Buckets: latencyBuckets,
		}),
		DeleteFileFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_delete_file_failure_latency_seconds",
			Help:    "Latency of failed DeleteFile operations",
			Buckets: latencyBuckets,
		}),

		ListDirectorySuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_list_directory_successes_total",
			Help: "Total number of successful ListDirectory operations",
		}),
		ListDirectoryFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_list_directory_failures_total",
			Help: "Total number of failed ListDirectory operations",
		}),
		ListDirectorySuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_list_directory_success_latency_seconds",
			Help:    "Latency of successful ListDirectory operations",
			Buckets: latencyBuckets,
		}),
		ListDirectoryFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_list_directory_failure_latency_seconds",
			Help:    "Latency of failed ListDirectory operations",
			Buckets: latencyBuckets,
		}),

		ListObjectsSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_list_objects_successes_total",
			Help: "Total number of successful ListObjects operations",
		}),
		ListObjectsFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_list_objects_failures_total",
			Help: "Total number of failed ListObjects operations",
		}),
		ListObjectsSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_list_objects_success_latency_seconds",
			Help:    "Latency of successful ListObjects operations",
			Buckets: latencyBuckets,
		}),
		ListObjectsFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_list_objects_failure_latency_seconds",
			Help:    "Latency of failed ListObjects operations",
			Buckets: latencyBuckets,
		}),

		ReadFileSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_read_file_successes_total",
			Help: "Total number of successful ReadFile operations",
		}),
		ReadFileFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_read_file_failures_total",
			Help: "Total number of failed ReadFile operations",
		}),
		ReadFileSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_read_file_success_latency_seconds",
			Help:    "Latency of successful ReadFile operations",
			Buckets: latencyBuckets,
		}),
		ReadFileFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_read_file_failure_latency_seconds",
			Help:    "Latency of failed ReadFile operations",
			Buckets: latencyBuckets,
		}),

		StatDirectorySuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_stat_directory_successes_total",
			Help: "Total number of successful StatDirectory operations",
		}),
		StatDirectoryFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_stat_directory_failures_total",
			Help: "Total number of failed StatDirectory operations",
		}),
		StatDirectorySuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_stat_directory_success_latency_seconds",
			Help:    "Latency of successful StatDirectory operations",
			Buckets: latencyBuckets,
		}),
		StatDirectoryFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_stat_directory_failure_latency_seconds",
			Help:    "Latency of failed StatDirectory operations",
			Buckets: latencyBuckets,
		}),

		StatFileSuccesses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_stat_file_successes_total",
			Help: "Total number of successful StatFile operations",
		}),
		StatFileFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "backend_stat_file_failures_total",
			Help: "Total number of failed StatFile operations",
		}),
		StatFileSuccessLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_stat_file_success_latency_seconds",
			Help:    "Latency of successful StatFile operations",
			Buckets: latencyBuckets,
		}),
		StatFileFailureLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_stat_file_failure_latency_seconds",
			Help:    "Latency of failed StatFile operations",
			Buckets: latencyBuckets,
		}),

		DirectoryPrefetchLatencies: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "backend_directory_prefetch_latency_seconds",
			Help:    "Latency of directory prefetch operations",
			Buckets: latencyBuckets,
		}),
	}

	return
}
