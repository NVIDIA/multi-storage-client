package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) failed: %v\n", err)
	}

	switch parsedURL.Scheme {
	case "http":
		// ok
	case "https":
		globals.logger.Fatalf("[FATAL] globals.config.endpoint specifies .Scheme: \"https\" - not currently supported")
	default:
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) returned invalid .Scheme: \"%s\"", parsedURL.Scheme)
	}

	if (parsedURL.Path != "") && (parsedURL.Path != "/") {
		globals.logger.Fatalf("[FATAL] url.Parse(globals.config.endpoint) returned non-empty .Path: \"%s\"", parsedURL.Path)
	}

	go func(parsedURL *url.URL) {
		var (
			err error
		)

		err = http.ListenAndServe(parsedURL.Host, &globals)
		if err != nil {
			globals.logger.Fatalf("[FATAL]  http.ListenAndServe() failed: %v", err)
		}
	}(parsedURL)

	globals.logger.Printf("[INFO] endpoint: %s://%s", parsedURL.Scheme, parsedURL.Host)
}

func (*globalsStruct) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		backend     *backendStruct
		backendName string
		registry    *prometheus.Registry
	)

	switch {
	case r.RequestURI == "/backends":
		w.WriteHeader(http.StatusOK)

		globals.Lock()

		for _, backend = range globals.config.backends {
			fmt.Fprintf(w, "%s\n", backend.dirName)
		}

		globals.Unlock()

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
	}
}

func registerFissionMetrics(registry *prometheus.Registry, m *fissionMetricsStruct) {
	registry.MustRegister(m.LookupSuccesses)
	registry.MustRegister(m.LookupFailures)
	registry.MustRegister(m.LookupSuccessLatencies)
	registry.MustRegister(m.LookupFailureLatencies)
	registry.MustRegister(m.GetAttrSuccesses)
	registry.MustRegister(m.GetAttrFailures)
	registry.MustRegister(m.GetAttrSuccessLatencies)
	registry.MustRegister(m.GetAttrFailureLatencies)
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
	registry.MustRegister(m.ReleaseDirSuccesses)
	registry.MustRegister(m.ReleaseDirFailures)
	registry.MustRegister(m.ReleaseDirSuccessLatencies)
	registry.MustRegister(m.ReleaseDirFailureLatencies)
	registry.MustRegister(m.ReadDirPlusSuccesses)
	registry.MustRegister(m.ReadDirPlusFailures)
	registry.MustRegister(m.ReadDirPlusSuccessLatencies)
	registry.MustRegister(m.ReadDirPlusFailureLatencies)
	registry.MustRegister(m.StatXSuccesses)
	registry.MustRegister(m.StatXFailures)
	registry.MustRegister(m.StatXSuccessLatencies)
	registry.MustRegister(m.StatXFailureLatencies)
}

func registerBackendMetrics(registry *prometheus.Registry, m *backendMetricsStruct) {
	registry.MustRegister(m.DeleteFileSuccesses)
	registry.MustRegister(m.DeleteFileFailures)
	registry.MustRegister(m.DeleteFileSuccessLatencies)
	registry.MustRegister(m.DeleteFileFailureLatencies)
	registry.MustRegister(m.ListDirectorySuccesses)
	registry.MustRegister(m.ListDirectoryFailures)
	registry.MustRegister(m.ListDirectorySuccessLatencies)
	registry.MustRegister(m.ListDirectoryFailureLatencies)
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
}
