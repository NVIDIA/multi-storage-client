package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
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
		backend     *backendStruct
		backendName string
		numDrained  uint64
		registry    *prometheus.Registry
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
			fmt.Fprintf(w, "  <li><a href=\"/metrics\">/metrics</a></li>\n")
			globals.Lock()
			for _, backend = range globals.config.backends {
				fmt.Fprintf(w, "  <li><a href=\"/metrics/%s\">/metrics/%s</a></li>\n", backend.dirName, backend.dirName)
			}
			globals.Unlock()
			fmt.Fprintf(w, "</ul>\n</body>\n</html>\n")
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Endpoints:\n")
			fmt.Fprintf(w, "  /backends\n")
			fmt.Fprintf(w, "  /drain\n")
			fmt.Fprintf(w, "  /dump\n")
			fmt.Fprintf(w, "  /metrics\n")
			globals.Lock()
			for _, backend = range globals.config.backends {
				fmt.Fprintf(w, "  /metrics/%s\n", backend.dirName)
			}
			globals.Unlock()
		}
	case r.RequestURI == "/backends":
		w.WriteHeader(http.StatusOK)

		globals.Lock()

		for _, backend = range globals.config.backends {
			fmt.Fprintf(w, "%s\n", backend.dirName)
		}

		globals.Unlock()

	case r.RequestURI == "/drain":
		globals.Lock()

		numDrained = inodeEvictorForceDrain()

		globals.Unlock()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%v\n", numDrained)

	case r.RequestURI == "/dump":
		w.WriteHeader(http.StatusOK)
		dumpFS(w)

	case r.RequestURI == "/metrics":
		registry = prometheus.NewRegistry()

		globals.Lock()

		registerFissionMetrics(registry, globals.fissionMetrics)
		registerBackendMetrics(registry, globals.backendMetrics)

		globals.Unlock()

		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)

	case strings.HasPrefix(r.RequestURI, "/metrics/"):
		backendName = strings.TrimPrefix(r.RequestURI, "/metrics/")
		if backendName == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "backend name required\n")
			return
		}

		globals.Lock()

		backend = globals.config.backends[backendName]
		if backend == nil {
			globals.Unlock()
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "backend %q not found\n", backendName)
			return
		}

		registry = prometheus.NewRegistry()

		registerFissionMetrics(registry, backend.fissionMetrics)
		registerBackendMetrics(registry, backend.backendMetrics)

		globals.Unlock()

		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)

	default:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "unknown endpoint - must be one of:\n")
		fmt.Fprintf(w, "  /backends\n")
		fmt.Fprintf(w, "  /drain\n")
		fmt.Fprintf(w, "  /dump\n")
		fmt.Fprintf(w, "  /metrics\n")
		globals.Lock()
		for _, backend = range globals.config.backends {
			fmt.Fprintf(w, "  /metrics/%s\n", backend.dirName)
		}
		globals.Unlock()
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
