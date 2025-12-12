package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NVIDIA/multi-storage-client/multi-storage-file-system/telemetry"
	"github.com/NVIDIA/multi-storage-client/multi-storage-file-system/telemetry/attributes"
	"github.com/NVIDIA/multi-storage-client/multi-storage-file-system/telemetry/auth"
)

// `main` is the entrypoint for the FUSE file system daemon. It parses the
// command line. Help text will be output if explicitly requested or the
// command line arguments are not understood. In other cases, it requires
// a successful parsing of the configuration file whose location is
// determined in the initGlobals() call. Next, the FUSE file system is
// initialized and the configuration file specified backends are mounted
// beneath the root of the FUSE file system. The daemon then enters a loop
// until receiving a SIGINT or SIGTERM. Either periodically or in response
// to a SIGHUP, the configuration file is re-read and the list of backends
// is adjusted based on any changes detected.
func main() {
	var (
		displayHelp            bool
		displayHelpMatchSet    map[string]struct{}
		err                    error
		errLastCheckConfigFile error
		osArgs                 []string // Copy of os.Args so that initGlobals() can be passed a modified set of arguments in testing/benchmarking
		signalChan             chan os.Signal
		signalReceived         os.Signal
		ticker                 *time.Ticker
	)

	osArgs = make([]string, len(os.Args))
	_ = copy(osArgs, os.Args)

	displayHelpMatchSet = make(map[string]struct{})
	displayHelpMatchSet["-?"] = struct{}{}
	displayHelpMatchSet["-h"] = struct{}{}
	displayHelpMatchSet["help"] = struct{}{}
	displayHelpMatchSet["-help"] = struct{}{}
	displayHelpMatchSet["--help"] = struct{}{}
	displayHelpMatchSet["-v"] = struct{}{}
	displayHelpMatchSet["-version"] = struct{}{}
	displayHelpMatchSet["--version"] = struct{}{}

	switch len(osArgs) {
	case 1:
		displayHelp = false
	case 2:
		_, displayHelp = displayHelpMatchSet[osArgs[1]]
	default:
		displayHelp = true
	}

	if displayHelp {
		fmt.Printf("usage: %s [{-?|-h|help|-help|--help|-v|-version|--version} | <config-file>]\n", osArgs[0])
		fmt.Printf("  where a <config-file>, ending in suffix .yaml, .yml, or .json is to be found while searching:\n")
		fmt.Printf("    ${MSC_CONFIG}\n")
		fmt.Printf("    ${XDG_CONFIG_HOME}/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    ${HOME}/.msc_config.{yaml|yml|json}\n")
		fmt.Printf("    ${HOME}/.config/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    ${XDG_CONFIG_DIRS:-/etc/xdg}/msc/config.{yaml|yml|json}\n")
		fmt.Printf("    /etc/msc_config.{yaml|yml|json}\n")
		fmt.Printf("version:\n")
		fmt.Printf("  %s\n", GitTag)
		os.Exit(0)
	}

	initGlobals(osArgs)

	err = checkConfigFile()
	if err != nil {
		globals.logger.Fatalf("[FATAL] parsing config-file (\"%s\") failed: %v", globals.configFilePath, err)
	}

	// Initialize observability (metrics, tracing, logging)
	initObservability()

	initFS()

	processToMountList()

	err = performFissionMount()
	if err != nil {
		globals.logger.Fatalf("[FATAL] unable to perform FUSE mount [Err: %v]", err)
	}

	signalChan = make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	if globals.config.autoSIGHUPInterval == 0 {
		ticker = time.NewTicker(365 * 24 * time.Hour)
		ticker.Stop()
	} else {
		ticker = time.NewTicker(globals.config.autoSIGHUPInterval)
	}

	errLastCheckConfigFile = nil

	for {
		select {
		case signalReceived = <-signalChan:
			if signalReceived != syscall.SIGHUP {
				// We received either syscall.SIGINT or syscall.SIGTERM...so terminate normally

				err = performFissionUnmount()
				if err != nil {
					globals.logger.Fatalf("[FATAL] unexpected error during FUSE unmount: %v", err)
				}

				drainFS()

				// Shutdown observability (flush pending metrics)
				if globals.meterProvider != nil {
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if mp, ok := globals.meterProvider.(interface{ Shutdown(context.Context) error }); ok {
						if err := mp.Shutdown(shutdownCtx); err != nil {
							globals.logger.Printf("[WARN] error shutting down meter provider: %v", err)
						} else {
							globals.logger.Printf("[INFO] meter provider shut down successfully")
						}
					}
					cancel()
				}

				os.Exit(0)
			}

			// We received a syscall.SIGHUP... so re-parse (current) content of globals.condfigFilePath and resume

			err = checkConfigFile()
			if err == nil {
				globals.logger.Printf("[INFO] parsing config-file (\"%s\") succeeded", globals.configFilePath)

				processToUnmountList()

				processToMountList()
			} else {
				globals.logger.Printf("[WARN] parsing config-file (\"%s\") failed: %v", globals.configFilePath, err)
			}

			errLastCheckConfigFile = err
		case <-ticker.C:
			// Act like we received a syscall.SIGHUP... so re-parse (current) content of globals.condfigFilePath and resume

			err = checkConfigFile()
			if err == nil {
				if errLastCheckConfigFile != nil {
					globals.logger.Printf("[INFO] parsing config-file (\"%s\") succeeded", globals.configFilePath)
				}

				processToUnmountList()

				processToMountList()
			} else if (errLastCheckConfigFile == nil) || (errLastCheckConfigFile.Error() != err.Error()) {
				globals.logger.Printf("[WARN] parsing config-file (\"%s\") failed: %v", globals.configFilePath, err)
			}

			errLastCheckConfigFile = err
		case err = <-globals.errChan:
			// We received an Unexpected exit of /dev/fuse read loop... to terminate abnormally

			globals.logger.Fatalf("[FATAL] received unexpected FUSE error: %v", err)
		}
	}
}

// initObservability initializes metrics via OTLP for MSCP.
// Config structure matches MSC Python schema exactly: opentelemetry.metrics.{attributes, reader, exporter}
// Logs are written to stdout (redirected to /var/log/msc/mscp_*.log by mount.msc).
func initObservability() {
	// Check if observability is configured
	if globals.config.observability == nil {
		globals.logger.Printf("[INFO] observability not configured, skipping initialization")
		return
	}

	// Check if metrics exporter is configured (matches Python schema requirement)
	if globals.config.observability.metricsExporter == nil {
		globals.logger.Printf("[INFO] metrics exporter not configured, skipping metrics initialization")
		return
	}

	// Extract configuration from Python-compatible schema
	exporterType := globals.config.observability.metricsExporter.Type
	exporterOptions := globals.config.observability.metricsExporter.Options

	// Get reader options with defaults (matching Python defaults)
	collectIntervalMs := uint64(1000) // 1 second default
	collectTimeoutMs := uint64(10000) // 10 seconds default
	exportIntervalMs := uint64(60000) // 60 seconds default
	exportTimeoutMs := uint64(30000)  // 30 seconds default

	if globals.config.observability.metricsReaderOptions != nil {
		collectIntervalMs = globals.config.observability.metricsReaderOptions.CollectIntervalMillis
		collectTimeoutMs = globals.config.observability.metricsReaderOptions.CollectTimeoutMillis
		exportIntervalMs = globals.config.observability.metricsReaderOptions.ExportIntervalMillis
		exportTimeoutMs = globals.config.observability.metricsReaderOptions.ExportTimeoutMillis
	}

	// Process attribute providers (matches Python: instantiate providers from config)
	attributeProviders := processAttributeProviders(globals.config.observability.metricsAttributes)

	// Create metrics config based on exporter type
	var metricsConfig telemetry.MetricsConfig
	metricsConfig.Enabled = true
	metricsConfig.CollectIntervalMillis = collectIntervalMs
	metricsConfig.CollectTimeoutMillis = collectTimeoutMs
	metricsConfig.ExportIntervalMillis = exportIntervalMs
	metricsConfig.ExportTimeoutMillis = exportTimeoutMs
	metricsConfig.ServiceName = "msc-posix"
	metricsConfig.AttributeProviders = attributeProviders

	// Handle different exporter types
	switch exporterType {
	case "otlp":
		// Standard OTLP exporter (no auth)
		endpoint, ok := exporterOptions["endpoint"].(string)
		if !ok {
			globals.logger.Printf("[WARN] metrics exporter endpoint not configured, skipping metrics initialization")
			return
		}

		// Check for insecure option (HTTP vs HTTPS)
		insecure := true // default to insecure for dev
		if insecureVal, ok := exporterOptions["insecure"].(bool); ok {
			insecure = insecureVal
		}

		metricsConfig.OTLPEndpoint = endpoint
		metricsConfig.Insecure = insecure

	case "_otlp_msal":
		// OTLP with Azure MSAL authentication
		// Config structure: auth{client_id, client_credential, authority, scopes} + exporter{endpoint}
		authOptions, ok := exporterOptions["auth"].(map[string]interface{})
		if !ok {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'auth' configuration, skipping metrics initialization")
			return
		}

		exporterSubOptions, ok := exporterOptions["exporter"].(map[string]interface{})
		if !ok {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'exporter' configuration, skipping metrics initialization")
			return
		}

		// Extract and validate auth config
		clientID, ok := authOptions["client_id"].(string)
		if !ok || clientID == "" {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'auth.client_id', skipping metrics initialization")
			return
		}

		clientCredential, ok := authOptions["client_credential"].(string)
		if !ok || clientCredential == "" {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'auth.client_credential', skipping metrics initialization")
			return
		}

		authority, ok := authOptions["authority"].(string)
		if !ok || authority == "" {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'auth.authority', skipping metrics initialization")
			return
		}

		var scopes []string
		if scopesInterface, ok := authOptions["scopes"].([]interface{}); ok {
			for _, s := range scopesInterface {
				if scope, ok := s.(string); ok && scope != "" {
					scopes = append(scopes, scope)
				}
			}
		}
		if len(scopes) == 0 {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires at least one 'auth.scopes', skipping metrics initialization")
			return
		}

		// Extract and validate endpoint from nested exporter config
		endpoint, ok := exporterSubOptions["endpoint"].(string)
		if !ok || endpoint == "" {
			globals.logger.Printf("[WARN] _otlp_msal exporter requires 'exporter.endpoint', skipping metrics initialization")
			return
		}

		metricsConfig.OTLPEndpoint = endpoint
		metricsConfig.Insecure = false // MSAL always uses HTTPS
		metricsConfig.AzureAuth = &auth.Config{
			ClientID:         clientID,
			ClientCredential: clientCredential,
			Authority:        authority,
			Scopes:           scopes,
		}

	default:
		globals.logger.Printf("[WARN] unsupported metrics exporter type: %s (supported: 'otlp', '_otlp_msal')", exporterType)
		return
	}

	// Initialize metrics with diperiodic pattern
	meterProvider, metricAttrs, err := telemetry.SetupMetricsDiperiodic(metricsConfig)
	if err != nil {
		globals.logger.Printf("[WARN] failed to initialize metrics: %v", err)
		return
	}

	globals.logger.Printf("[INFO] metrics initialized with diperiodic pattern (collect=%dms, export=%dms), sending to %s",
		collectIntervalMs, exportIntervalMs, metricsConfig.OTLPEndpoint)

	// Create MSCP metrics instruments (matches MSC Python: gauges use LastValue, counters use Sum)
	// Pass metricAttrs so they're added to every metric recording (matching Python behavior)
	metrics, err := telemetry.NewMSCPMetricsDiperiodic("msc-posix", metricAttrs)
	if err != nil {
		globals.logger.Printf("[WARN] failed to create metrics instruments: %v", err)
		return
	}

	globals.metrics = metrics
	globals.meterProvider = meterProvider // Store for shutdown later
	globals.logger.Printf("[INFO] metrics instruments created successfully")
}

// processAttributeProviders instantiates attribute providers from configuration.
// Matches Python: providers/base.py:_init_metrics() attribute provider instantiation
func processAttributeProviders(configs []attributeProviderStruct) []attributes.AttributesProvider {
	var providers []attributes.AttributesProvider

	// Map of type names to provider constructors
	// Matches Python: _TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING
	providerMapping := map[string]func(map[string]interface{}) attributes.AttributesProvider{
		"static": func(opts map[string]interface{}) attributes.AttributesProvider {
			return attributes.NewStaticAttributesProvider(opts)
		},
		"host": func(opts map[string]interface{}) attributes.AttributesProvider {
			return attributes.NewHostAttributesProvider(opts)
		},
		"process": func(opts map[string]interface{}) attributes.AttributesProvider {
			return attributes.NewProcessAttributesProvider(opts)
		},
		"environment_variables": func(opts map[string]interface{}) attributes.AttributesProvider {
			return attributes.NewEnvironmentVariablesAttributesProvider(opts)
		},
		"msc_config": func(opts map[string]interface{}) attributes.AttributesProvider {
			// Pass the full config dictionary for JMESPath queries
			// Add config_dict to options if not already present
			if _, ok := opts["config_dict"]; !ok {
				opts["config_dict"] = globals.configFileMap
			}
			return attributes.NewMSCConfigAttributesProvider(opts)
		},
	}

	providers = make([]attributes.AttributesProvider, 0, len(configs))

	for _, config := range configs {
		// Look up provider constructor
		constructor, ok := providerMapping[config.Type]
		if !ok {
			globals.logger.Printf("[WARN] unknown attribute provider type: %s, skipping", config.Type)
			continue
		}

		// Instantiate provider with options
		provider := constructor(config.Options)
		providers = append(providers, provider)

		globals.logger.Printf("[INFO] initialized attribute provider: %s", config.Type)
	}

	return providers
}
