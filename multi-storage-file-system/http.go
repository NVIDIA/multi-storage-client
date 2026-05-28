package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	HTTP_SERVER_READ_TIMEOUT  = 10 * time.Second
	HTTP_SERVER_WRITE_TIMEOUT = 10 * time.Second
	HTTP_SERVER_IDLE_TIMEOUT  = 10 * time.Second
)

func formatLockDuration(duration time.Duration) string {
	var (
		absDuration time.Duration
		unit        string
		value       float64
	)

	absDuration = duration
	if absDuration < 0 {
		absDuration = -absDuration
	}

	switch {
	case absDuration >= time.Second:
		unit = "s"
		value = float64(duration) / float64(time.Second)
	case absDuration >= time.Millisecond:
		unit = "ms"
		value = float64(duration) / float64(time.Millisecond)
	case absDuration >= time.Microsecond:
		unit = "us"
		value = float64(duration) / float64(time.Microsecond)
	default:
		unit = "ns"
		value = float64(duration)
	}

	return fmt.Sprintf("%.2f %-2s", value, unit)
}

func startHTTPHandler() {
	var (
		err       error
		parsedURL *url.URL
	)

	if globals.config.endpoint == "" {
		globals.logger.Printf("[INFO] no endpoint specified")
		return
	}

	parsedURL, err = url.Parse(globals.config.endpoint)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) failed: %v\n", err)
	}

	switch parsedURL.Scheme {
	case "http":
		// ok
	case "https":
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.config.endpoint specifies .Scheme: \"https\" - not currently supported")
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) returned invalid .Scheme: \"%s\"", parsedURL.Scheme)
	}

	if (parsedURL.Path != "") && (parsedURL.Path != "/") {
		dumpStack()
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) returned non-empty .Path: \"%s\"", parsedURL.Path)
	}

	go func(parsedURL *url.URL) {
		var (
			err                    error
			httpServer             *http.Server
			httpServerLoggerLogger = log.New(globals.logger.Writer(), "[HTTP-SERVER] ", globals.logger.Flags()) // set prefix to differentiate httpServer logging
		)

		httpServer = &http.Server{
			Addr:         parsedURL.Host,
			Handler:      &globals,
			ReadTimeout:  HTTP_SERVER_READ_TIMEOUT,
			WriteTimeout: HTTP_SERVER_WRITE_TIMEOUT,
			IdleTimeout:  HTTP_SERVER_IDLE_TIMEOUT,
			ErrorLog:     httpServerLoggerLogger,
		}

		err = httpServer.ListenAndServe()
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] httpServer.ListenAndServe() failed: %v", err)
		}
	}(parsedURL)

	globals.logger.Printf("[INFO] endpoint: %s://%s", parsedURL.Scheme, parsedURL.Host)
}

func (*globalsStruct) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		backend                   *backendStruct
		backendName               string
		backendNames              []string
		globalsLockMaxHoldEntries []GlobalsLockMaxHoldEntry
		globalsLockMaxHoldEntry   GlobalsLockMaxHoldEntry
		globalsLockedHolderSite   string
		holdAvgAsString           string
		holdAvgAsStringLen        int
		holdAvgAsStringMaxLen     int
		holdCntAsString           string
		holdCntMax                uint64
		holdCntMaxAsStringLen     int
		holdMaxAsString           string
		holdMaxAsStringLen        int
		holdMaxAsStringMaxLen     int
		holdSumAsString           string
		holdSumAsStringLen        int
		holdSumAsStringMaxLen     int
		locksSortDirective        string
		numDrained                uint64
		registry                  *prometheus.Registry
	)

	switch {
	case r.RequestURI == "/":
		if strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "<!DOCTYPE html>\n<html>\n<head><title>MSFS Endpoints</title></head>\n<body>\n")
			fmt.Fprintf(w, "<h1>Endpoints</h1>\n<ul>\n")
			fmt.Fprintf(w, "  <li><a href=\"/backends\">/backends</a></li>\n")
			fmt.Fprintf(w, "  <li><a href=\"/drain\">/drain</a></li>\n")
			fmt.Fprintf(w, "  <li><a href=\"/dump\">/dump</a></li>\n")
			fmt.Fprintf(w, "  <li><a href=\"/hang\">/hang</a></li>\n")
			fmt.Fprintf(w, "  <li><a href=\"/locks\">/locks</a></li>\n")
			fmt.Fprintf(w, "  <li><a href=\"/metrics\">/metrics</a></li>\n")
			globalsLock("http.go:150:4:(*globalsStruct).ServeHTTP")
			backendNames = make([]string, 0, len(globals.config.backends))
			for _, backend = range globals.config.backends {
				backendNames = append(backendNames, backend.dirName)
			}
			globalsUnlock()
			slices.Sort(backendNames)
			for _, backendName = range backendNames {
				fmt.Fprintf(w, "  <li><a href=\"/metrics/%s\">/metrics/%s</a></li>\n", backendName, backendName)
			}
			fmt.Fprintf(w, "</ul>\n</body>\n</html>\n")
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Endpoints:\n")
			fmt.Fprintf(w, "  /backends\n")
			fmt.Fprintf(w, "  /drain\n")
			fmt.Fprintf(w, "  /dump\n")
			fmt.Fprintf(w, "  /hang\n")
			fmt.Fprintf(w, "  /locks\n")
			fmt.Fprintf(w, "  /metrics\n")
			globalsLock("http.go:170:4:(*globalsStruct).ServeHTTP")
			backendNames = make([]string, 0, len(globals.config.backends))
			for _, backend = range globals.config.backends {
				backendNames = append(backendNames, backend.dirName)
			}
			globalsUnlock()
			slices.Sort(backendNames)
			for _, backendName = range backendNames {
				fmt.Fprintf(w, "  /metrics/%s\n", backendName)
			}
		}
	case r.RequestURI == "/backends":
		w.WriteHeader(http.StatusOK)

		globalsLock("http.go:184:3:(*globalsStruct).ServeHTTP")

		for _, backend = range globals.config.backends {
			fmt.Fprintf(w, "%s\n", backend.dirName)
		}

		globalsUnlock()

	case r.RequestURI == "/drain":
		globalsLock("http.go:193:3:(*globalsStruct).ServeHTTP")

		numDrained = inodeEvictorForceDrain()

		globalsUnlock()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%v\n", numDrained)

	case r.RequestURI == "/dump":
		w.WriteHeader(http.StatusOK)
		dumpFS(w)

	case r.RequestURI == "/hang":
		globalsLockedHolderSite = GlobalsLockHolderSite()
		w.WriteHeader(http.StatusOK)
		if globalsLockedHolderSite == "" {
			fmt.Fprintf(w, "no hang detected\n")
		} else {
			fmt.Fprintf(w, "possible hang due to %s\n", globalsLockedHolderSite)
		}

	case r.URL.Path == "/locks":
		locksSortDirective = r.URL.Query().Get("sort")
		if locksSortDirective == "" {
			locksSortDirective = "sum"
		}

		globalsLock("http.go:221:3:(*globalsStruct).ServeHTTP")
		globalsLockMaxHoldEntries = GlobalsLockMaxHoldDurations()
		globalsUnlock()

		switch locksSortDirective {
		case "site":
			SortGlobalsLockMaxHoldEntriesBySite(globalsLockMaxHoldEntries)
		case "cnt":
			SortGlobalsLockMaxHoldEntriesByHoldCnt(globalsLockMaxHoldEntries)
		case "sum":
			SortGlobalsLockMaxHoldEntriesByHoldSum(globalsLockMaxHoldEntries)
		case "max":
			SortGlobalsLockMaxHoldEntriesByHoldMax(globalsLockMaxHoldEntries)
		case "avg":
			SortGlobalsLockMaxHoldEntriesByHoldAvg(globalsLockMaxHoldEntries)
		default:
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "bad sort directive - must be one of: site, cnt, sum, max, avg\n")
			return
		}

		w.WriteHeader(http.StatusOK)
		holdCntMax = 0
		holdSumAsStringMaxLen = 0
		holdMaxAsStringMaxLen = 0
		holdAvgAsStringMaxLen = 0
		for _, globalsLockMaxHoldEntry = range globalsLockMaxHoldEntries {
			if globalsLockMaxHoldEntry.HoldCnt == 0 {
				continue
			}
			if holdCntMax < globalsLockMaxHoldEntry.HoldCnt {
				holdCntMax = globalsLockMaxHoldEntry.HoldCnt
			}
			holdSumAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldSum)
			holdSumAsStringLen = len(holdSumAsString)
			if holdSumAsStringMaxLen < holdSumAsStringLen {
				holdSumAsStringMaxLen = holdSumAsStringLen
			}
			holdMaxAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldMax)
			holdMaxAsStringLen = len(holdMaxAsString)
			if holdMaxAsStringMaxLen < holdMaxAsStringLen {
				holdMaxAsStringMaxLen = holdMaxAsStringLen
			}
			holdAvgAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldAvg)
			holdAvgAsStringLen = len(holdAvgAsString)
			if holdAvgAsStringMaxLen < holdAvgAsStringLen {
				holdAvgAsStringMaxLen = holdAvgAsStringLen
			}
		}
		holdCntMaxAsStringLen = len(strconv.FormatUint(holdCntMax, 10))
		for _, globalsLockMaxHoldEntry = range globalsLockMaxHoldEntries {
			if globalsLockMaxHoldEntry.HoldCnt == 0 {
				continue
			}
			holdCntAsString = strconv.FormatUint(globalsLockMaxHoldEntry.HoldCnt, 10)
			holdSumAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldSum)
			holdMaxAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldMax)
			holdAvgAsString = formatLockDuration(globalsLockMaxHoldEntry.HoldAvg)
			fmt.Fprintf(w, "    %*s cnt %*s sum %*s max %*s avg %*s\n",
				globalsLockMaxSiteKeyLen, globalsLockMaxHoldEntry.Site,
				holdCntMaxAsStringLen, holdCntAsString,
				holdSumAsStringMaxLen, holdSumAsString,
				holdMaxAsStringMaxLen, holdMaxAsString,
				holdAvgAsStringMaxLen, holdAvgAsString)
		}

	case r.RequestURI == "/metrics":
		registry = prometheus.NewRegistry()

		globalsLock("http.go:290:3:(*globalsStruct).ServeHTTP")

		registerFissionMetrics(registry, globals.fissionMetrics)
		registerBackendMetrics(registry, globals.backendMetrics)
		registerGlobalsLockMetrics(registry)

		globalsUnlock()

		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)

	case strings.HasPrefix(r.RequestURI, "/metrics/"):
		backendName = strings.TrimPrefix(r.RequestURI, "/metrics/")
		if backendName == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "backend name required\n")
			return
		}

		globalsLock("http.go:308:3:(*globalsStruct).ServeHTTP")

		backend = globals.config.backends[backendName]
		if backend == nil {
			globalsUnlock()
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "backend %q not found\n", backendName)
			return
		}

		registry = prometheus.NewRegistry()

		registerFissionMetrics(registry, backend.fissionMetrics)
		registerBackendMetrics(registry, backend.backendMetrics)

		globalsUnlock()

		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)

	default:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "unknown endpoint - must be one of:\n")
		fmt.Fprintf(w, "  /backends\n")
		fmt.Fprintf(w, "  /drain\n")
		fmt.Fprintf(w, "  /dump\n")
		fmt.Fprintf(w, "  /hang\n")
		fmt.Fprintf(w, "  /locks\n")
		fmt.Fprintf(w, "  /metrics\n")
		globalsLock("http.go:336:3:(*globalsStruct).ServeHTTP")
		for _, backend = range globals.config.backends {
			fmt.Fprintf(w, "  /metrics/%s\n", backend.dirName)
		}
		globalsUnlock()
	}
}

func registerFissionMetrics(registry *prometheus.Registry, m *fissionMetricsStruct) {
	if m == nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] registerFissionMetrics() passed a nil *fissionMetricsStruct")
	}
	registry.MustRegister(m.LookupSuccesses)
	registry.MustRegister(m.LookupFailures)
	registry.MustRegister(m.LookupSuccessLatencies)
	registry.MustRegister(m.LookupFailureLatencies)
	registry.MustRegister(m.GetAttrSuccesses)
	registry.MustRegister(m.GetAttrFailures)
	registry.MustRegister(m.GetAttrSuccessLatencies)
	registry.MustRegister(m.GetAttrFailureLatencies)
	registry.MustRegister(m.MkNodSuccesses)
	registry.MustRegister(m.MkNodFailures)
	registry.MustRegister(m.MkNodSuccessLatencies)
	registry.MustRegister(m.MkNodFailureLatencies)
	registry.MustRegister(m.MkDirSuccesses)
	registry.MustRegister(m.MkDirFailures)
	registry.MustRegister(m.MkDirSuccessLatencies)
	registry.MustRegister(m.MkDirFailureLatencies)
	registry.MustRegister(m.UnlinkSuccesses)
	registry.MustRegister(m.UnlinkFailures)
	registry.MustRegister(m.UnlinkSuccessLatencies)
	registry.MustRegister(m.UnlinkFailureLatencies)
	registry.MustRegister(m.RmDirSuccesses)
	registry.MustRegister(m.RmDirFailures)
	registry.MustRegister(m.RmDirSuccessLatencies)
	registry.MustRegister(m.RmDirFailureLatencies)
	registry.MustRegister(m.OpenSuccesses)
	registry.MustRegister(m.OpenFailures)
	registry.MustRegister(m.OpenSuccessLatencies)
	registry.MustRegister(m.OpenFailureLatencies)
	registry.MustRegister(m.ReadSuccesses)
	registry.MustRegister(m.ReadFailures)
	registry.MustRegister(m.ReadSuccessLatencies)
	registry.MustRegister(m.ReadFailureLatencies)
	registry.MustRegister(m.ReadSuccessSizes)
	registry.MustRegister(m.ReadFailureSizes)
	registry.MustRegister(m.ReadCacheHits)
	registry.MustRegister(m.ReadCacheMisses)
	registry.MustRegister(m.ReadCacheWaits)
	registry.MustRegister(m.ReadCachePrefetches)
	registry.MustRegister(m.StatFSCalls)
	registry.MustRegister(m.ReleaseSuccesses)
	registry.MustRegister(m.ReleaseFailures)
	registry.MustRegister(m.ReleaseSuccessLatencies)
	registry.MustRegister(m.ReleaseFailureLatencies)
	registry.MustRegister(m.OpenDirSuccesses)
	registry.MustRegister(m.OpenDirFailures)
	registry.MustRegister(m.OpenDirSuccessLatencies)
	registry.MustRegister(m.OpenDirFailureLatencies)
	registry.MustRegister(m.ReadDirSuccesses)
	registry.MustRegister(m.ReadDirFailures)
	registry.MustRegister(m.ReadDirSuccessLatencies)
	registry.MustRegister(m.ReadDirFailureLatencies)
	registry.MustRegister(m.ReadDirEntriesReturned)
	registry.MustRegister(m.ReleaseDirSuccesses)
	registry.MustRegister(m.ReleaseDirFailures)
	registry.MustRegister(m.ReleaseDirSuccessLatencies)
	registry.MustRegister(m.ReleaseDirFailureLatencies)
	registry.MustRegister(m.CreateSuccesses)
	registry.MustRegister(m.CreateFailures)
	registry.MustRegister(m.CreateSuccessLatencies)
	registry.MustRegister(m.CreateFailureLatencies)
	registry.MustRegister(m.ReadDirPlusSuccesses)
	registry.MustRegister(m.ReadDirPlusFailures)
	registry.MustRegister(m.ReadDirPlusSuccessLatencies)
	registry.MustRegister(m.ReadDirPlusFailureLatencies)
	registry.MustRegister(m.ReadDirPlusEntriesReturned)
	registry.MustRegister(m.StatXSuccesses)
	registry.MustRegister(m.StatXFailures)
	registry.MustRegister(m.StatXSuccessLatencies)
	registry.MustRegister(m.StatXFailureLatencies)
}

func registerBackendMetrics(registry *prometheus.Registry, m *backendMetricsStruct) {
	if m == nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] registerBackendMetrics() passed a nil *backendMetricsStruct")
	}
	registry.MustRegister(m.DeleteFileSuccesses)
	registry.MustRegister(m.DeleteFileFailures)
	registry.MustRegister(m.DeleteFileSuccessLatencies)
	registry.MustRegister(m.DeleteFileFailureLatencies)
	registry.MustRegister(m.ListDirectorySuccesses)
	registry.MustRegister(m.ListDirectoryFailures)
	registry.MustRegister(m.ListDirectorySuccessLatencies)
	registry.MustRegister(m.ListDirectoryFailureLatencies)
	registry.MustRegister(m.ListObjectsSuccesses)
	registry.MustRegister(m.ListObjectsFailures)
	registry.MustRegister(m.ListObjectsSuccessLatencies)
	registry.MustRegister(m.ListObjectsFailureLatencies)
	registry.MustRegister(m.ReadFileSuccesses)
	registry.MustRegister(m.ReadFileFailures)
	registry.MustRegister(m.ReadFileSuccessLatencies)
	registry.MustRegister(m.ReadFileFailureLatencies)
	registry.MustRegister(m.StatDirectorySuccesses)
	registry.MustRegister(m.StatDirectoryFailures)
	registry.MustRegister(m.StatDirectorySuccessLatencies)
	registry.MustRegister(m.StatDirectoryFailureLatencies)
	registry.MustRegister(m.StatFileSuccesses)
	registry.MustRegister(m.StatFileFailures)
	registry.MustRegister(m.StatFileSuccessLatencies)
	registry.MustRegister(m.StatFileFailureLatencies)
	registry.MustRegister(m.DirectoryPrefetchLatencies)
}
