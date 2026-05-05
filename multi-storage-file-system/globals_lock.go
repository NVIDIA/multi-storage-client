package main

// This file is excluded from tools/lockgen so the embedded sync.Mutex methods (globals.Lock,
// globals.TryLock, globals.Unlock) stay here; all other source should use globalsLock/globalsUnlock.
//
// Lock instrumentation: Prometheus metrics are canonical. A single atomic.Int64 tracks acquisition
// depth for Observe() samples and for prometheus.GaugeFunc (no duplicate Gauge Inc/Dec).

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// globalsLockSiteCount is the number of distinct lockgen site strings (unique globalsLock("…") call
// sites in this module). Maintained by: go generate (tools/lockgen).
const globalsLockSiteCount = 64

// globalsLockMaxSiteKeyLen is the length in bytes of the longest site string key in globalsLockMaxHoldBySite
// (len(s) for that key). Maintained by: go generate (tools/lockgen).
const globalsLockMaxSiteKeyLen = 70

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
	"backend.go:260:3:funcLit@259":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:314:3:funcLit@313":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:374:3:funcLit@373":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:435:3:funcLit@434":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:499:3:funcLit@498":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"backend.go:560:3:funcLit@559":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"bptree_test.go:59:3:BenchmarkBPTreePageInsertion":                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:129:3:(*cacheLineStruct).fetch":                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:147:2:(*cacheLineStruct).fetch":                                {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"cache.go:98:2:(*cacheLineStruct).fetch":                                 {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1033:3:(*globalsStruct).DoRead":                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1223:2:(*globalsStruct).DoStatFS":                            {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1261:3:funcLit@1259":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1280:2:(*globalsStruct).DoRelease":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1420:3:funcLit@1418":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1439:2:(*globalsStruct).DoOpenDir":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:153:3:funcLit@151":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1572:3:funcLit@1565":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1610:2:(*globalsStruct).DoReadDir":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:172:2:(*globalsStruct).DoLookup":                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1733:4:(*globalsStruct).DoReadDir":                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1849:3:funcLit@1847":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1868:2:(*globalsStruct).DoReleaseDir":                        {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1973:3:funcLit@1971":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:1992:2:(*globalsStruct).DoCreate":                            {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2194:3:funcLit@2187":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2234:2:(*globalsStruct).DoReadDirPlus":                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2357:4:(*globalsStruct).DoReadDirPlus":                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2494:3:funcLit@2492":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:2513:2:(*globalsStruct).DoStatX":                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:294:3:funcLit@292":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:313:2:(*globalsStruct).DoGetAttr":                            {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:434:3:funcLit@432":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:453:2:(*globalsStruct).DoMkDir":                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:557:3:funcLit@555":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:576:2:(*globalsStruct).DoUnlink":                             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:673:3:funcLit@671":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:692:2:(*globalsStruct).DoRmDir":                              {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:839:3:funcLit@837":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:858:2:(*globalsStruct).DoOpen":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission.go:973:3:funcLit@971":                                           {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:1228:2:TestFissionDoUnlinkRollbackOnBackendFailure":     {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:1618:2:TestFissionConvertPhysicalToVirtual":             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:1644:2:TestFissionConvertPhysicalToVirtual":             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:1680:2:TestFissionConvertPhysicalToVirtual":             {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:461:2:TestFissionDoGetAttrStatX":                        {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fission_test.go:641:2:TestFissionDoOpenDirReadDirReadDirPlusReleaseDir": {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1095:2:prefetchDirectory":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1124:3:prefetchDirectory":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:128:2:drainFS":                                                    {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1290:2:dumpFS":                                                    {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:1454:2:(*inodeStruct).finishPendingDelete":                        {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:170:2:processToMountList":                                         {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:246:2:processToUnmountList":                                       {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:24:2:initFS":                                                      {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"fs.go:825:4:inodeEvictor":                                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:114:4:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:134:4:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:148:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:157:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:180:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:216:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:234:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
	"http.go:262:3:(*globalsStruct).ServeHTTP":                               {HoldCnt: 0, HoldSum: 0, HoldMax: 0},
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
	HoldAvg time.Duration
	HoldMax time.Duration
}

// GlobalsLockMaxHoldDurations returns a snapshot of per-site stats (map iteration order), including HoldAvg.
// Caller must hold globals (globalsLock). After globalsUnlock(), use SortGlobalsLockMaxHoldEntriesByHoldAvg
// to order by highest average hold first.
func GlobalsLockMaxHoldDurations() []GlobalsLockMaxHoldEntry {
	n := len(globalsLockMaxHoldBySite)
	out := make([]GlobalsLockMaxHoldEntry, 0, n)
	for site, st := range globalsLockMaxHoldBySite {
		e := GlobalsLockMaxHoldEntry{Site: site, HoldCnt: st.HoldCnt, HoldMax: st.HoldMax}
		if st.HoldCnt > 0 {
			e.HoldAvg = time.Duration(int64(st.HoldSum) / int64(st.HoldCnt))
		}
		out = append(out, e)
	}
	return out
}

// SortGlobalsLockMaxHoldEntriesByHoldAvg sorts entries in place by HoldAvg descending (highest average first).
func SortGlobalsLockMaxHoldEntriesByHoldAvg(entries []GlobalsLockMaxHoldEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].HoldAvg > entries[j].HoldAvg })
}
