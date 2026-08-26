package main

// This file is excluded from tools/lockgen so the embedded sync.Mutex methods (globals.Lock,
// globals.TryLock, globals.Unlock) stay here; all other source should use globalsLock/globalsUnlock.
//
// Lock instrumentation: Prometheus metrics are canonical. A single atomic.Int64 tracks acquisition
// depth for Observe() samples and for prometheus.GaugeFunc (no duplicate Gauge Inc/Dec).

import (
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// globalsLockSiteCount is the number of distinct lockgen site strings (unique globalsLock("…") call
// sites in this module). Maintained by: go generate (tools/lockgen).
const globalsLockSiteCount = 107

// globalsLockMaxSiteKeyLen is the length in bytes of the longest site string key in globalsLockMaxHoldBySite
// (len(s) for that key). Maintained by: go generate (tools/lockgen).
const globalsLockMaxSiteKeyLen = 88

func init() {
	globalsLockHolderSite.Store("")
}

// globalsLockHolderSite holds the lockgen "site" label for whoever currently holds globals (empty if unlocked).
var globalsLockHolderSite atomic.Value // string

// globalsMuHoldStart records when the mutex was last acquired; only read/write while the embedded
// sync.Mutex is held (set at end of globalsLock, read at start of globalsUnlock).
var globalsMuHoldStart time.Time

// globalsLockAcquisitionDepth counts goroutines between globalsLock entry and successful mutex acquire
// (Inc before try, Dec after). Exposed to Prometheus only via globalsLockAcquisitionWaitersGaugeFunc.
var globalsLockAcquisitionDepth atomic.Int64

// globalsLockSiteStats is per-site instrumentation: hold count, sum of holds, largest single hold; average is HoldSum/HoldCnt.
type globalsLockSiteStats struct {
	HoldCnt uint64
	HoldSum time.Duration
	HoldMax time.Duration
}

// globalsLockMaxHoldBySite records per-site hold stats (count, sum, max). Keys are prefilled by
// lockgen; values are updated from globalsUnlock. Reads and copies require holding globals (globalsLock).
// lockgen-begin: globalsLockMaxHoldBySite
var globalsLockMaxHoldBySite = map[string]globalsLockSiteStats{
	"backend.go:312:3:funcLit@311":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:366:3:funcLit@365":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:427:4:funcLit@426":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:567:3:funcLit@566":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:631:3:funcLit@630":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:692:3:funcLit@691":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"bptree_test.go:60:3:BenchmarkBPTreePageInsertion":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:433:3:allocateDataCacheLines":                                                    {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:485:2:(*dataCacheLineTrackerStruct).fetch":                                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:517:2:(*dataCacheLineTrackerStruct).fetch":                                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1018:2:(*globalsStruct).DoOpen":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1226:3:(*globalsStruct).DoRead":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1324:4:(*globalsStruct).DoRead":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1588:2:(*globalsStruct).DoWrite":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1656:2:(*globalsStruct).DoStatFS":                                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1696:3:funcLit@1694":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1715:2:(*globalsStruct).DoRelease":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:174:3:funcLit@172":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1792:4:(*globalsStruct).DoRelease":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1833:2:(*globalsStruct).DoFSync":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1920:2:(*globalsStruct).DoFlush":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:193:2:(*globalsStruct).DoLookup":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1974:3:funcLit@1972":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1993:2:(*globalsStruct).DoOpenDir":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2134:3:funcLit@2127":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2172:2:(*globalsStruct).DoReadDir":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2295:4:(*globalsStruct).DoReadDir":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2411:3:funcLit@2409":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2430:2:(*globalsStruct).DoReleaseDir":                                          {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2543:3:funcLit@2541":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2562:2:(*globalsStruct).DoCreate":                                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2822:3:funcLit@2815":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2862:2:(*globalsStruct).DoReadDirPlus":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:3146:4:(*globalsStruct).DoReadDirPlus":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:315:3:funcLit@313":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:3283:3:funcLit@3281":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:3302:2:(*globalsStruct).DoStatX":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:334:2:(*globalsStruct).DoGetAttr":                                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:427:2:(*globalsStruct).DoSetAttr":                                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:586:3:funcLit@584":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:605:2:(*globalsStruct).DoMkDir":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:710:3:funcLit@708":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:729:2:(*globalsStruct).DoUnlink":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:832:3:funcLit@830":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:851:2:(*globalsStruct).DoRmDir":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:999:3:funcLit@997":                                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:1758:2:TestFissionDoUnlinkRollbackOnBackendFailure":                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:2148:2:TestFissionConvertPhysicalToVirtual":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:2174:2:TestFissionConvertPhysicalToVirtual":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:2210:2:TestFissionConvertPhysicalToVirtual":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:2312:2:TestFissionDoReadFetchFailureReturnsEIO":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:2344:2:TestFissionDoReadFetchFailureReturnsEIO":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:474:2:TestFissionDoGetAttrStatX":                                          {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:709:2:TestFissionDoOpenDirReadDirReadDirPlusReleaseDir":                   {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1147:2:prefetchDirectory":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1176:3:prefetchDirectory":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1342:2:dumpFS":                                                                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:136:2:drainFS":                                                                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1506:2:(*inodeStruct).finishPendingDelete":                                          {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:182:2:processToMountList":                                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:23:2:initFS":                                                                        {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:264:2:processToUnmountList":                                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:869:4:inodeEvictor":                                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:150:4:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:170:4:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:184:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:193:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:221:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:290:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:308:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:336:3:(*globalsStruct).ServeHTTP":                                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"manifest_ingest.go:246:2:ingestWriteBatch":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"manifest_ingest_delta_test.go:346:2:TestManifestDeltaDerivesParentWhenParentInodeMissing": {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write.go:454:3:funcLit@453":                                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache.go:150:2:(*writeCachePromotionJob).run":                                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:121:2:TestWriteCachePromotionDisabledLeavesCacheEmpty":                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:136:2:TestWriteCachePromotionStopsAtCapacity":                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:162:2:TestWriteCachePromotionEvictsOldestCleanLine":                   {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:167:2:TestWriteCachePromotionEvictsOldestCleanLine":                   {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:186:2:TestWriteCachePromotionDoesNotWaitForInboundLines":              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:244:2:TestWriteCachePromotionFromMultipartParts":                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:268:2:TestWriteCachePromotionFromCompleteOverlayLines":                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:305:2:TestWriteCachePromotionDiscardsStaleInboundFetch":               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:326:2:TestWriteCachePromotionDiscardsStaleInboundFetch":               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:337:2:TestWriteCachePromotionDiscardsStaleInboundFetch":               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:60:2:writeCacheTestInode":                                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:67:2:readPromotedLine":                                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_cache_test.go:97:4:funcLit@91":                                                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit.go:115:2:(*writeCommitJob).run":                                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit.go:288:3:waitForWriteCommitLocked":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit.go:322:2:(*inodeStruct).flushWriteFileConcurrentlyLocked":                    {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:167:2:TestSmallWriteCommitWaitsAndAppliesResult":                     {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:186:3:funcLit@185":                                                   {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:209:2:TestSmallWriteCommitWaitsAndAppliesResult":                     {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:236:2:TestSmallWriteCommitFailurePreservesRetryState":                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:278:2:TestSmallWriteCommitFailurePreservesRetryState":                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:357:2:TestAbandonedWriteCommitReleasesTheInode":                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:379:2:TestAbandonedWriteCommitReleasesTheInode":                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:425:2:TestFailedDetachedCommitSurfacesOnceAtBarrier":                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:430:2:TestFailedDetachedCommitSurfacesOnceAtBarrier":                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:435:2:TestFailedDetachedCommitSurfacesOnceAtBarrier":                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:448:2:TestFailedDetachedCommitSurfacesOnceAtBarrier":                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:468:2:TestSucceedingCommitClearsLatchedFailure":                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:495:2:TestSucceedingCommitClearsLatchedFailure":                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:515:2:TestSmallWriteCommitReleasesBudgetWhenInodeDisappears":         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:556:2:TestSmallWriteCommitReleasesBudgetWhenInodeDisappears":         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"write_commit_test.go:79:2:TestWriteCommitPoolBoundsConcurrency":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
}

// lockgen-end: globalsLockMaxHoldBySite

var globalsMutexLatencyBuckets = []float64{
	.000005, .000010, .000025, .000050, .000100, .000250, .000500, .001000, .002500, .005000, .010000,
	.025000, .050000, .100000, .250000, .500000, 1, 2.5, 5, 10,
}

var globalsLockContentionWaitersHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "msfs",
	Subsystem: "globals_mutex",
	Name:      "contention_waiters",
	Help:      "Sampled count of goroutines in the lock acquisition path when globalsLock runs (approximates queue depth entering acquire).",
	Buckets: []float64{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 20, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024,
	},
})

var globalsLockHoldDurationSecondsHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "msfs",
	Subsystem: "globals_mutex",
	Name:      "hold_duration_seconds",
	Help:      "Time the embedded sync.Mutex was held per critical section (seconds).",
	Buckets:   globalsMutexLatencyBuckets,
})

var globalsLockAcquireDurationSeconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "msfs",
		Subsystem: "globals_mutex",
		Name:      "acquire_duration_seconds",
		Help:      "Time to acquire the global mutex: result=nonblocking (TryLock) or blocking (Lock).",
		Buckets:   globalsMutexLatencyBuckets,
	},
	[]string{"result"},
)

// Live acquisition depth; same values as globalsLockAcquisitionDepth (GaugeFunc reads the atomic).
var globalsLockAcquisitionWaitersGaugeFunc = prometheus.NewGaugeFunc(
	prometheus.GaugeOpts{
		Namespace: "msfs",
		Subsystem: "globals_mutex",
		Name:      "acquisition_waiters",
		Help:      "Number of goroutines currently in the globalsLock acquisition path (after entry, before mutex held).",
	},
	func() float64 { return float64(globalsLockAcquisitionDepth.Load()) },
)

func registerGlobalsLockMetrics(registry *prometheus.Registry) {
	registry.MustRegister(globalsLockContentionWaitersHist)
	registry.MustRegister(globalsLockHoldDurationSecondsHist)
	registry.MustRegister(globalsLockAcquireDurationSeconds)
	registry.MustRegister(globalsLockAcquisitionWaitersGaugeFunc)
}

func observeContentionWaiters(after int64) {
	globalsLockContentionWaitersHist.Observe(float64(after))
}

// globalsLock acquires the embedded sync.Mutex on globals and records Prometheus metrics.
func globalsLock(site string) {
	after := globalsLockAcquisitionDepth.Add(1)

	start := time.Now()
	if globals.TryLock() {
		globalsLockAcquisitionDepth.Add(-1)
		globalsLockHolderSite.Store(site)
		globalsMuHoldStart = time.Now()
		globalsLockAcquireDurationSeconds.WithLabelValues("nonblocking").Observe(time.Since(start).Seconds())
		observeContentionWaiters(after)
		return
	}
	globals.Lock()
	wait := time.Since(start)
	globalsLockAcquisitionDepth.Add(-1)
	globalsLockHolderSite.Store(site)
	globalsMuHoldStart = time.Now()
	globalsLockAcquireDurationSeconds.WithLabelValues("blocking").Observe(wait.Seconds())
	observeContentionWaiters(after)
}

func globalsUnlock() {
	hold := time.Since(globalsMuHoldStart)
	globalsLockHoldDurationSecondsHist.Observe(hold.Seconds())

	site := GlobalsLockHolderSite()
	if site == "" {
		dumpStack()
		globals.logger.Fatalf("globalsUnlock: empty holder site (unlock without matching globalsLock?)")
	}
	st, exists := globalsLockMaxHoldBySite[site]
	if !exists {
		dumpStack()
		globals.logger.Fatalf("globalsUnlock: globalsLockMaxHoldBySite[site] returned !exists")
	}
	st.HoldCnt++
	st.HoldSum += hold
	if hold > st.HoldMax {
		st.HoldMax = hold
	}
	globalsLockMaxHoldBySite[site] = st

	globalsLockHolderSite.Store("")
	globals.Unlock()
}

// GlobalsLockHolderSite returns the lockgen site label for the goroutine currently holding globals, or ""
// if the mutex is not held. Safe to call without globals locked (e.g. from a debug HTTP handler).
func GlobalsLockHolderSite() string {
	v := globalsLockHolderSite.Load()
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// GlobalsLockMaxHoldEntry is one lockgen site with aggregate hold stats (HoldAvg is HoldSum/HoldCnt when HoldCnt > 0).
type GlobalsLockMaxHoldEntry struct {
	Site    string
	HoldCnt uint64
	HoldSum time.Duration
	HoldMax time.Duration
	HoldAvg time.Duration
}

// GlobalsLockMaxHoldDurations returns a snapshot of per-site stats (map iteration order), including HoldAvg.
// Caller must hold globals (globalsLock). After globalsUnlock(), use SortGlobalsLockMaxHoldEntriesByHoldAvg
// to order by highest average hold first.
func GlobalsLockMaxHoldDurations() []GlobalsLockMaxHoldEntry {
	n := len(globalsLockMaxHoldBySite)
	out := make([]GlobalsLockMaxHoldEntry, 0, n)
	for site, st := range globalsLockMaxHoldBySite {
		e := GlobalsLockMaxHoldEntry{Site: site, HoldCnt: st.HoldCnt, HoldSum: st.HoldSum, HoldMax: st.HoldMax}
		if st.HoldCnt > 0 {
			e.HoldAvg = time.Duration(int64(st.HoldSum) / int64(st.HoldCnt))
		} else {
			e.HoldAvg = time.Duration(0)
		}
		out = append(out, e)
	}
	return out
}

func globalsLockSiteSortKey(site string) (filePath string, lineNumber uint64, remainder string) {
	var (
		lineNumberAsString string
		ok                 bool
	)

	filePath, remainder, ok = strings.Cut(site, ":")
	if !ok {
		return
	}

	lineNumberAsString, remainder, ok = strings.Cut(remainder, ":")
	if !ok {
		lineNumberAsString = remainder
		remainder = ""
	}

	lineNumber, _ = strconv.ParseUint(lineNumberAsString, 10, 64)

	return
}

// SortGlobalsLockMaxHoldEntriesBySite sorts entries in place by file path, line number, then remainder.
func SortGlobalsLockMaxHoldEntriesBySite(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool {
		iFilePath, iLineNumber, iRemainder := globalsLockSiteSortKey(entries[i].Site)
		jFilePath, jLineNumber, jRemainder := globalsLockSiteSortKey(entries[j].Site)

		if iFilePath != jFilePath {
			return iFilePath < jFilePath
		}
		if iLineNumber != jLineNumber {
			return iLineNumber < jLineNumber
		}
		return iRemainder < jRemainder
	})
}

// SortGlobalsLockMaxHoldEntriesByHoldCnt sorts entries in place by HoldCnt descending.
func SortGlobalsLockMaxHoldEntriesByHoldCnt(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].HoldCnt > entries[j].HoldCnt })
}

// SortGlobalsLockMaxHoldEntriesByHoldSum sorts entries in place by HoldSum descending.
func SortGlobalsLockMaxHoldEntriesByHoldSum(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].HoldSum > entries[j].HoldSum })
}

// SortGlobalsLockMaxHoldEntriesByHoldMax sorts entries in place by HoldMax descending.
func SortGlobalsLockMaxHoldEntriesByHoldMax(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].HoldMax > entries[j].HoldMax })
}

// SortGlobalsLockMaxHoldEntriesByHoldAvg sorts entries in place by HoldAvg descending.
func SortGlobalsLockMaxHoldEntriesByHoldAvg(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].HoldAvg > entries[j].HoldAvg })
}
