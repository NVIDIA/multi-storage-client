package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultMountPoint              = "/mnt"
	defaultRAMMaxTotalObjects      = uint64(10000)
	defaultRAMMaxTotalObjectSpace  = uint64(1073741824) // 2^30 == 1Gi
	defaultRAMMaxDirectoryPageSize = uint64(100)
)

// `parseAny` provides a convenient test for the existence of
// a key string in the map.
func parseAny(m map[string]interface{}, key string) (ok bool) {
	_, ok = m[key]
	return
}

// `parseAnyOf` provides a convenient test for the existence of
// any of the supplied key strings in the map.
func parseAnyOf(m map[string]interface{}, keySet []string) (ok bool) {
	var (
		key string
	)

	ok = false // Handles the case where len(keySet) == 0

	for _, key = range keySet {
		_, ok = m[key]
		if ok {
			return
		}
	}

	return // If we make it to here, ok remains false
}

// `parseBool` fetches what is expected to be a bool value for the
// specified key from the map. If the key is missing and a non-nil
// dflt is provided, the func will return this dflt.
func parseBool(m map[string]interface{}, key string, dflt interface{}) (b, ok bool) {
	var (
		v interface{}
	)

	v, ok = m[key]
	if ok {
		b, ok = v.(bool)
		return
	}

	if dflt == nil {
		ok = false
		return
	}

	b, ok = dflt.(bool)

	return
}

// `parseFloat64` fetches what is expected to be a float64 value for the
// specified key from the map. If the key is missing and a non-nil
// dflt is provided, the func will return this dflt.
func parseFloat64(m map[string]interface{}, key string, dflt interface{}) (f float64, ok bool) {
	var (
		i int
		v interface{}
	)

	v, ok = m[key]
	if ok {
		f, ok = v.(float64)
		if !ok {
			i, ok = v.(int)
			if ok {
				f = float64(i)
			}
		}
		return
	}

	if dflt == nil {
		ok = false
		return
	}

	f, ok = dflt.(float64)

	return
}

// `parseMilliseconds` fetches what is expected to be a uint64 value for the
// specified key from the map converting it to a time.Duration assuming the
// uint64 specifies a number of milliseconds. If the key is missing and a
// non-nil dflt is provided, the func will return this dflt.
func parseMilliseconds(m map[string]interface{}, key string, dflt interface{}) (d time.Duration, ok bool) {
	var (
		dDflt   time.Duration
		uDflt   uint64
		uParsed uint64
	)

	dDflt, ok = dflt.(time.Duration)

	if ok {
		uDflt = uint64(dDflt) / uint64(time.Millisecond)
		uParsed, ok = parseUint64(m, key, uDflt)
	} else {
		uParsed, ok = parseUint64(m, key, nil)
	}

	if !ok {
		return
	}

	d = time.Duration(uParsed * uint64(time.Millisecond))

	return
}

// `parseSeconds` fetches what is expected to be a uint64 value for the
// specified key from the map converting it to a time.Duration assuming the
// uint64 specifies a number of seconds. If the key is missing and a
// non-nil dflt is provided, the func will return this dflt.
func parseSeconds(m map[string]interface{}, key string, dflt interface{}) (d time.Duration, ok bool) {
	var (
		dDflt   time.Duration
		uDflt   uint64
		uParsed uint64
	)

	dDflt, ok = dflt.(time.Duration)

	if ok {
		uDflt = uint64(dDflt) / uint64(time.Second)
		uParsed, ok = parseUint64(m, key, uDflt)
	} else {
		uParsed, ok = parseUint64(m, key, nil)
	}

	if !ok {
		return
	}

	d = time.Duration(uParsed * uint64(time.Second))

	return
}

// `parseString` fetches what is expected to be a string value for the
// specified key from the map. If the key is missing and a non-nil
// dflt is provided, the dflt value will be used. In either case of
// a value to be returned, it will be expanded with environment variable
// substitutions, if any, before being returned.
func parseString(m map[string]interface{}, key string, dflt interface{}) (s string, ok bool) {
	var (
		v interface{}
	)

	v, ok = m[key]
	if ok {
		s, ok = v.(string)
		if ok {
			s = os.ExpandEnv(s)
		}
		return
	}

	if dflt == nil {
		ok = false
		return
	}

	s, ok = dflt.(string)
	if ok {
		s = os.ExpandEnv(s)
	}

	return
}

// `parseUint64` fetches what is expected to be a uint64 value for the
// specified key from the map. If the key is missing and a non-nil
// dflt is provided, the func will return this dflt.
func parseUint64(m map[string]interface{}, key string, dflt interface{}) (u uint64, ok bool) {
	var (
		f float64
		i int
		v interface{}
	)

	v, ok = m[key]
	if ok {
		f, ok = v.(float64)
		if ok {
			u = uint64(f)
			ok = (float64(u) == f)
			return
		}

		i, ok = v.(int)
		if ok {
			u = uint64(i)
			ok = (int(u) == i)
			return
		}

		u, ok = v.(uint64)

		return
	}

	if dflt == nil {
		ok = false
		return
	}

	u, ok = dflt.(uint64)

	return
}

// `checkConfigFile` parses globals.configFilePath in either JSON or YAML
// format following either the MSC Python-compatible or MSFS-specific
// specification. Upon success, it will also populate both the
// globals.backendsToUnmount and globals.backendsToMount lists in the
// case where an existing configuration is being updated.
func checkConfigFile() (err error) {
	var (
		backendAsInterface                    interface{}
		backendsAsInterface                   interface{}
		backendsAsInterfaceSlice              []interface{}
		backendsAsInterfaceSliceIndex         int
		backendAsMap                          map[string]interface{}
		backendAsStructNew                    *backendStruct
		backendAsStructOld                    *backendStruct
		backendConfigRAMAsInterface           interface{}
		backendConfigRAMAsMap                 map[string]interface{}
		backendConfigRAMAsStruct              *backendConfigRAMStruct
		backendConfigS3AsInterface            interface{}
		backendConfigS3AsMap                  map[string]interface{}
		backendConfigS3AsStruct               *backendConfigS3Struct
		config                                *configStruct
		configFileContent                     []byte
		configFileMap                         map[string]interface{}
		configFileMapTranslated               map[string]interface{}
		configFilePathExt                     string
		credentialsProviderAsInterface        interface{}
		credentialsProviderAsMap              map[string]interface{}
		credentialsProviderOptionsAsInterface interface{}
		credentialsProviderOptionsAsMap       map[string]interface{}
		credentialsProviderOptionsAccessKey   string
		credentialsProviderOptionsSecretKey   string
		credentialsProviderType               string
		dirName                               string
		dirPerm                               string
		dirtyCacheLinesFlushTriggerPercentage uint64
		dirtyCacheLinesMaxPercentage          uint64
		filePerm                              string
		nextRetryDelay                        time.Duration
		ok                                    bool
		posixAllowOther                       bool
		posixAsInterface                      interface{}
		posixAsMap                            map[string]interface{}
		posixAutoSIGHUPInterval               uint64
		posixMountname                        string
		posixMountpoint                       string
		profileAsInterface                    interface{}
		profileAsMap                          map[string]interface{}
		profileName                           string
		profilesAsInterface                   interface{}
		profilesAsMap                         map[string]interface{}
		storageProviderAsInterface            interface{}
		storageProviderAsMap                  map[string]interface{}
		storageProviderOptionsAsInterface     interface{}
		storageProviderOptionsAsMap           map[string]interface{}
		storageProviderOptionsBasePath        string
		storageProviderOptionsBasePathPrefix  string
		storageProviderOptionsBasePathSplit   []string
		storageProviderOptionsEndpointURL     string
		storageProviderOptionsRegionName      string
		storageProviderType                   string
	)

	// Compute configFileMap

	configFileContent, err = os.ReadFile(globals.configFilePath)
	if err != nil {
		err = fmt.Errorf("unable to read config-file: %v", err)
		return
	}

	configFileMap = make(map[string]interface{})

	configFilePathExt = filepath.Ext(globals.configFilePath)

	switch configFilePathExt {
	case ".json":
		err = json.Unmarshal(configFileContent, &configFileMap)
		if err != nil {
			err = fmt.Errorf("unable to parse config-file \"%s\" as JSON (err: %v)", globals.configFilePath, err)
			return
		}
	case ".yaml", ".yml":
		err = yaml.Unmarshal(configFileContent, &configFileMap)
		if err != nil {
			err = fmt.Errorf("unable to parse config-file \"%s\" as YAML (err: %v)", globals.configFilePath, err)
			return
		}
	default:
		err = fmt.Errorf("unsupported extension (\"%s\") in config-file \"%s\" - must be one of \".json\" or \".yaml\"", configFilePathExt, globals.configFilePath)
		return
	}

	config = &configStruct{
		backends: make(map[string]*backendStruct),
	}

	config.msfsVersion, ok = parseUint64(configFileMap, "msfs_version", uint64(0))
	if !ok {
		err = errors.New("bad msfs_version value")
		return
	}

	switch config.msfsVersion {
	case MSFSVersionPythonCompatibility:
		profilesAsInterface, ok = configFileMap["profiles"]
		if ok && (profilesAsInterface != nil) {
			profilesAsMap, ok = profilesAsInterface.(map[string]interface{})
			if !ok {
				err = errors.New("bad profiles section")
				return
			}

			backendsAsInterfaceSlice = make([]interface{}, 0, len(profilesAsMap))

			for profileName, profileAsInterface = range profilesAsMap {
				profileAsMap, ok = profileAsInterface.(map[string]interface{})
				if !ok {
					err = fmt.Errorf("bad profile \"%s\"", profileName)
					return
				}

				storageProviderAsInterface, ok = profileAsMap["storage_provider"]
				if !ok {
					// Skip this one as storageProvider not supported
					_, ok = globals.backendsSkipped[profileName]
					if !ok {
						globals.logger.Printf("[INFO] skipping profile \"%s\" with no storage_provider", profileName)
						globals.backendsSkipped[profileName] = struct{}{}
					}
					continue
				}
				storageProviderAsMap, ok = storageProviderAsInterface.(map[string]interface{})
				if !ok {
					err = fmt.Errorf("bad profile \"%s\" storage_provider", profileName)
					return
				}

				storageProviderType, ok = parseString(storageProviderAsMap, "type", nil)
				if !ok {
					err = fmt.Errorf("missing or bad profile \"%s\" storage_provider type", profileName)
					return
				}
				switch storageProviderType {
				case "s3":
					// This one is supported
				case "s8k":
					// This is compatible with "s3", so simply operate as if storageProviderType == "s3"
				default:
					// Skip this one as storageProviderType not currently supported
					_, ok = globals.backendsSkipped[profileName]
					if !ok {
						globals.logger.Printf("[INFO] skipping profile \"%s\" with storage_provider \"%s\"", profileName, storageProviderType)
						globals.backendsSkipped[profileName] = struct{}{}
					}
					continue
				}

				backendAsMap = make(map[string]interface{})

				backendAsMap["dir_name"] = profileName

				backendConfigS3AsMap = make(map[string]interface{})

				storageProviderOptionsAsInterface, ok = storageProviderAsMap["options"]
				if !ok {
					err = fmt.Errorf("missing profile \"%s\" storage_provider options", profileName)
					return
				}
				storageProviderOptionsAsMap, ok = storageProviderOptionsAsInterface.(map[string]interface{})
				if !ok {
					err = fmt.Errorf("bad profile \"%s\" storage_provider options", profileName)
					return
				}

				storageProviderOptionsBasePath, ok = parseString(storageProviderOptionsAsMap, "base_path", nil)
				if !ok {
					err = fmt.Errorf("missing or bad profile \"%s\" storage_provider options base_path", profileName)
					return
				}

				storageProviderOptionsBasePathSplit = strings.Split(storageProviderOptionsBasePath, "/")
				switch len(storageProviderOptionsBasePathSplit) {
				case 0:
					err = fmt.Errorf("bad profile \"%s\" storage_provider options base_path [empty]", profileName)
					return
				case 1:
					backendAsMap["bucket_container_name"] = storageProviderOptionsBasePathSplit[0]
					backendAsMap["prefix"] = ""
				default:
					backendAsMap["bucket_container_name"] = storageProviderOptionsBasePathSplit[0]
					storageProviderOptionsBasePathPrefix = strings.Join(storageProviderOptionsBasePathSplit[1:], "/")
					if !strings.HasSuffix(storageProviderOptionsBasePathPrefix, "/") {
						storageProviderOptionsBasePathPrefix += "/"
					}
					backendAsMap["prefix"] = storageProviderOptionsBasePathPrefix
				}

				if parseAnyOf(storageProviderOptionsAsMap, []string{"endpoint_url", "region_name"}) {
					backendConfigS3AsMap["use_config_env"] = false // The default

					storageProviderOptionsRegionName, ok = parseString(storageProviderOptionsAsMap, "region_name", "")
					if ok {
						if storageProviderOptionsRegionName != "" {
							backendConfigS3AsMap["region"] = storageProviderOptionsRegionName
						}
					} else {
						err = fmt.Errorf("bad profile \"%s\" storage_provider options region_name", profileName)
						return
					}

					storageProviderOptionsEndpointURL, ok = parseString(storageProviderOptionsAsMap, "endpoint_url", "${AWS_ENDPOINT}")
					if ok {
						if storageProviderOptionsEndpointURL != "" {
							backendConfigS3AsMap["endpoint"] = storageProviderOptionsEndpointURL
						}
					} else {
						err = fmt.Errorf("bad profile \"%s\" storage_provider options endpoint_url", profileName)
						return
					}
				} else { // !parseAnyOf(storageProviderOptionsAsMap, []string{"endpoint_url", "region_name"})
					backendConfigS3AsMap["use_config_env"] = true
				}

				credentialsProviderAsInterface, ok = profileAsMap["credentials_provider"]
				if ok {
					backendConfigS3AsMap["use_credentials_env"] = false // The default

					credentialsProviderAsMap, ok = credentialsProviderAsInterface.(map[string]interface{})
					if !ok {
						err = fmt.Errorf("bad profile \"%s\" credentials_provider", profileName)
						return
					}

					credentialsProviderType, ok = parseString(credentialsProviderAsMap, "type", nil)
					if !ok {
						err = fmt.Errorf("missing or bad profile \"%s\" credentials_provider type", profileName)
						return
					}
					if credentialsProviderType != "S3Credentials" {
						err = fmt.Errorf("bad profile \"%s\" storage_provider type (\"%s\") - must be \"S3Credentials\"", profileName, credentialsProviderType)
						return
					}

					credentialsProviderOptionsAsInterface, ok = credentialsProviderAsMap["options"]
					if !ok {
						err = fmt.Errorf("missing profile \"%s\" credentials_provider options", profileName)
						return
					}
					credentialsProviderOptionsAsMap, ok = credentialsProviderOptionsAsInterface.(map[string]interface{})
					if !ok {
						err = fmt.Errorf("bad profile \"%s\" credentials_provider options", profileName)
						return
					}

					credentialsProviderOptionsAccessKey, ok = parseString(credentialsProviderOptionsAsMap, "access_key", "")
					if ok {
						if credentialsProviderOptionsAccessKey != "" {
							backendConfigS3AsMap["access_key_id"] = credentialsProviderOptionsAccessKey
						}
					} else {
						err = fmt.Errorf("bad profile \"%s\" credentials_provider options access_key", profileName)
						return
					}

					credentialsProviderOptionsSecretKey, ok = parseString(credentialsProviderOptionsAsMap, "secret_key", "")
					if ok {
						if credentialsProviderOptionsSecretKey != "" {
							backendConfigS3AsMap["secret_access_key"] = credentialsProviderOptionsSecretKey
						}
					} else {
						err = fmt.Errorf("bad profile \"%s\" credentials_provider options secret_key", profileName)
						return
					}
				} else { // profileAsMap["credentials_provider"] returned !ok
					backendConfigS3AsMap["use_credentials_env"] = true
				}

				backendAsMap["backend_type"] = "S3"
				backendAsMap["S3"] = backendConfigS3AsMap

				backendsAsInterfaceSlice = append(backendsAsInterfaceSlice, backendAsMap)
			}
		} else { // (configFileMap["profiles"] returned !ok) || (profilesAsInterface == nil)
			backendsAsInterfaceSlice = make([]interface{}, 0)
		}

		configFileMapTranslated = make(map[string]interface{})

		configFileMapTranslated["msfs_version"] = MSFSVersionOne
		configFileMapTranslated["backends"] = backendsAsInterfaceSlice

		// Preserve opentelemetry section if present (observability add-on)
		opentelemetryAsInterface, ok := configFileMap["opentelemetry"]
		if ok {
			configFileMapTranslated["opentelemetry"] = opentelemetryAsInterface
		}

		posixAsInterface, ok = configFileMap["posix"]
		if ok {
			posixAsMap, ok = posixAsInterface.(map[string]interface{})
			if ok {
				if parseAny(posixAsMap, "mountname") {
					posixMountname, ok = parseString(posixAsMap, "mountname", nil)
					if !ok {
						err = errors.New("bad posix mountname")
						return
					}

					configFileMapTranslated["mountname"] = posixMountname
				}

				if parseAny(posixAsMap, "mountpoint") {
					posixMountpoint, ok = parseString(posixAsMap, "mountpoint", nil)
					if !ok {
						err = errors.New("bad posix mountpoint")
						return
					}

					configFileMapTranslated["mountpoint"] = posixMountpoint
				}

				if parseAny(posixAsMap, "allow_other") {
					posixAllowOther, ok = parseBool(posixAsMap, "allow_other", nil)
					if !ok {
						err = errors.New("bad posix allow_other")
						return
					}

					configFileMapTranslated["allow_other"] = posixAllowOther
				}

				if parseAny(posixAsMap, "auto_sighup_interval") {
					posixAutoSIGHUPInterval, ok = parseUint64(posixAsMap, "auto_sighup_interval", nil)
					if !ok {
						err = errors.New("bad posix auto_sighup_interval")
						return
					}

					configFileMapTranslated["auto_sighup_interval"] = posixAutoSIGHUPInterval
				}
			}
		}

		configFileMap = configFileMapTranslated
	case MSFSVersionOne:
		// Nothing to do here
	default:
		err = fmt.Errorf("unsupported msfs_version: %v", config.msfsVersion)
		return
	}

	config.mountName, ok = parseString(configFileMap, "mountname", "msfs")
	if !ok {
		err = errors.New("bad mountname value")
		return
	}

	config.mountPoint = os.Getenv(EnvMSFSMountPoint)
	if config.mountPoint == "" {
		config.mountPoint, ok = parseString(configFileMap, "mountpoint", defaultMountPoint)
		if !ok {
			err = errors.New("bad mountpoint value")
			return
		}
	}

	config.uid, ok = parseUint64(configFileMap, "uid", uint64(os.Geteuid()))
	if !ok {
		err = errors.New("bad uid value")
		return
	}

	config.gid, ok = parseUint64(configFileMap, "gid", uint64(os.Getegid()))
	if !ok {
		err = errors.New("bad gid value")
		return
	}

	dirPerm, ok = parseString(configFileMap, "dir_perm", "555")
	if !ok {
		err = errors.New("bad perm value")
		return
	}
	config.dirPerm, err = strconv.ParseUint(dirPerm, 8, 64)
	if (err != nil) || (config.dirPerm > 0o777) {
		err = errors.New("bad dir_perm value")
		return
	}

	config.allowOther, ok = parseBool(configFileMap, "allow_other", true)
	if !ok {
		err = errors.New("bad allow_other value")
		return
	}

	config.maxWrite, ok = parseUint64(configFileMap, "max_write", uint64(131072))
	if !ok {
		err = errors.New("bad max_write value")
		return
	}

	config.entryAttrTTL, ok = parseMilliseconds(configFileMap, "entry_attr_ttl", 10000*time.Millisecond)
	if !ok {
		err = errors.New("bad entry_attr_ttl value")
		return
	}

	config.evictableInodeTTL, ok = parseMilliseconds(configFileMap, "evictable_inode_ttl", 1000000*time.Millisecond)
	if !ok {
		err = errors.New("bad evictable_inode_ttl value")
		return
	}
	if uint64(config.evictableInodeTTL) < uint64(config.entryAttrTTL) {
		err = fmt.Errorf("evictable_inode_ttl(%v) should be at least entry_attr_ttl(%v)", config.evictableInodeTTL, config.entryAttrTTL)
		return
	}

	config.cacheLineSize, ok = parseUint64(configFileMap, "cache_line_size", uint64(1048576))
	if !ok {
		err = errors.New("bad cache_line_size value")
		return
	}

	config.cacheLines, ok = parseUint64(configFileMap, "cache_lines", uint64(4096))
	if !ok {
		err = errors.New("bad cache_lines value")
		return
	}

	dirtyCacheLinesFlushTriggerPercentage, ok = parseUint64(configFileMap, "dirty_cache_lines_flush_trigger", uint64(80))
	if !ok {
		err = errors.New("missing or bad dirty_cache_lines_flush_trigger value")
		return
	}
	if dirtyCacheLinesFlushTriggerPercentage > 100 {
		err = errors.New("dirty_cache_lines_flush_trigger is a percentage so must be <= 100")
		return
	}
	config.dirtyCacheLinesFlushTrigger = (config.cacheLines * dirtyCacheLinesFlushTriggerPercentage) / uint64(100)

	dirtyCacheLinesMaxPercentage, ok = parseUint64(configFileMap, "dirty_cache_lines_max", uint64(90))
	if !ok {
		err = errors.New("missing or bad dirty_cache_lines_max value")
		return
	}
	if dirtyCacheLinesMaxPercentage > 100 {
		err = errors.New("dirty_cache_lines_max is a percentage so must be <= 100")
		return
	}
	if dirtyCacheLinesFlushTriggerPercentage > dirtyCacheLinesMaxPercentage {
		err = errors.New("dirty_cache_lines_flush_trigger must be <= dirty_cache_lines_max")
		return
	}
	config.dirtyCacheLinesMax = (config.cacheLines * dirtyCacheLinesMaxPercentage) / uint64(100)

	config.autoSIGHUPInterval, ok = parseSeconds(configFileMap, "auto_sighup_interval", time.Duration(0))
	if !ok {
		err = errors.New("bad auto_sighup_interval value")
		return
	}

	// Parse observability configuration (optional) - matches MSC Python's "opentelemetry" key exactly
	opentelemetryAsInterface, ok := configFileMap["opentelemetry"]
	if ok {
		opentelemetryAsMap, ok := opentelemetryAsInterface.(map[string]interface{})
		if !ok {
			err = errors.New("bad opentelemetry section")
			return
		}

		obs := &observabilityConfigStruct{}

		// Parse metrics section - matches Python schema: opentelemetry.metrics.{attributes, reader, exporter}
		metricsAsInterface, ok := opentelemetryAsMap["metrics"]
		if ok {
			metricsAsMap, ok := metricsAsInterface.(map[string]interface{})
			if ok {
				// Parse metrics.attributes (array of attribute providers)
				if attributesAsInterface, ok := metricsAsMap["attributes"]; ok {
					if attributesAsArray, ok := attributesAsInterface.([]interface{}); ok {
						for _, attrAsInterface := range attributesAsArray {
							if attrAsMap, ok := attrAsInterface.(map[string]interface{}); ok {
								attrProvider := attributeProviderStruct{}
								attrProvider.Type, _ = parseString(attrAsMap, "type", "")
								if optionsAsInterface, ok := attrAsMap["options"]; ok {
									if optionsAsMap, ok := optionsAsInterface.(map[string]interface{}); ok {
										attrProvider.Options = optionsAsMap
									}
								}
								obs.metricsAttributes = append(obs.metricsAttributes, attrProvider)
							}
						}
					}
				}

				// Parse metrics.reader.options
				if readerAsInterface, ok := metricsAsMap["reader"]; ok {
					if readerAsMap, ok := readerAsInterface.(map[string]interface{}); ok {
						if optionsAsInterface, ok := readerAsMap["options"]; ok {
							if optionsAsMap, ok := optionsAsInterface.(map[string]interface{}); ok {
								readerOpts := &readerOptionsStruct{}
								readerOpts.CollectIntervalMillis, _ = parseUint64(optionsAsMap, "collect_interval_millis", 1000)
								readerOpts.CollectTimeoutMillis, _ = parseUint64(optionsAsMap, "collect_timeout_millis", 10000)
								readerOpts.ExportIntervalMillis, _ = parseUint64(optionsAsMap, "export_interval_millis", 60000)
								readerOpts.ExportTimeoutMillis, _ = parseUint64(optionsAsMap, "export_timeout_millis", 30000)
								obs.metricsReaderOptions = readerOpts
							}
						}
					}
				}

				// Parse metrics.exporter (type + options)
				if exporterAsInterface, ok := metricsAsMap["exporter"]; ok {
					if exporterAsMap, ok := exporterAsInterface.(map[string]interface{}); ok {
						exporter := &exporterStruct{}
						exporter.Type, _ = parseString(exporterAsMap, "type", "")
						if optionsAsInterface, ok := exporterAsMap["options"]; ok {
							if optionsAsMap, ok := optionsAsInterface.(map[string]interface{}); ok {
								exporter.Options = optionsAsMap
							}
						}
						obs.metricsExporter = exporter
					}
				}
			}
		}

		config.observability = obs
	}

	backendsAsInterface, ok = configFileMap["backends"]
	if ok {
		backendsAsInterfaceSlice, ok = backendsAsInterface.([]interface{})
		if !ok {
			err = errors.New("bad backends section")
			return
		}

		for backendsAsInterfaceSliceIndex, backendAsInterface = range backendsAsInterfaceSlice {
			backendAsMap, ok = backendAsInterface.(map[string]interface{})
			if !ok {
				err = errors.New("bad backends section")
				return
			}

			backendAsStructNew = &backendStruct{}

			backendAsStructNew.dirName, ok = parseString(backendAsMap, "dir_name", nil)
			if !ok {
				err = fmt.Errorf("missing or bad dir_name at backends[%v]", backendsAsInterfaceSliceIndex)
				return
			}
			if (backendAsStructNew.dirName == DotDirEntryBasename) || (backendAsStructNew.dirName == DotDotDirEntryBasename) {
				err = fmt.Errorf("dir_name cannot be either \"%s\" or \"%s\"", DotDirEntryBasename, DotDotDirEntryBasename)
				return
			}

			backendAsStructNew.readOnly, ok = parseBool(backendAsMap, "readonly", true)
			if !ok {
				err = fmt.Errorf("bad readonly at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.flushOnClose, ok = parseBool(backendAsMap, "flush_on_close", true)
			if !ok {
				err = fmt.Errorf("bad flush_on_close at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.uid, ok = parseUint64(backendAsMap, "uid", uint64(os.Geteuid()))
			if !ok {
				err = fmt.Errorf("bad uid at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.gid, ok = parseUint64(backendAsMap, "gid", uint64(os.Getegid()))
			if !ok {
				err = fmt.Errorf("bad gid at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			if backendAsStructNew.readOnly {
				dirPerm, ok = parseString(backendAsMap, "dir_perm", "555")
			} else {
				dirPerm, ok = parseString(backendAsMap, "dir_perm", "777")
			}
			if !ok {
				err = fmt.Errorf("bad dir_perm at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}
			backendAsStructNew.dirPerm, err = strconv.ParseUint(dirPerm, 8, 64)
			if (err != nil) || (backendAsStructNew.dirPerm > 0o777) {
				err = fmt.Errorf("bad dir_perm at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			if backendAsStructNew.readOnly {
				filePerm, ok = parseString(backendAsMap, "file_perm", "444")
			} else {
				filePerm, ok = parseString(backendAsMap, "file_perm", "666")
			}
			if !ok {
				err = fmt.Errorf("bad file_perm at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}
			backendAsStructNew.filePerm, err = strconv.ParseUint(filePerm, 8, 64)
			if (err != nil) || (backendAsStructNew.filePerm > 0o777) {
				err = fmt.Errorf("bad file_perm at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.directoryPageSize, ok = parseUint64(backendAsMap, "directory_page_size", uint64(0))
			if !ok {
				err = fmt.Errorf("bad directory_page_size at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.multiPartCacheLineThreshold, ok = parseUint64(backendAsMap, "multipart_cache_line_threshold", uint64(512))
			if !ok {
				err = fmt.Errorf("bad multipart_cache_line_threshold at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.uploadPartCacheLines, ok = parseUint64(backendAsMap, "upload_part_cache_lines", uint64(32))
			if !ok {
				err = fmt.Errorf("bad upload_part_cache_lines at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.uploadPartConcurrency, ok = parseUint64(backendAsMap, "upload_part_concurrency", uint64(32))
			if !ok {
				err = fmt.Errorf("bad upload_part_concurrency at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.bucketContainerName, ok = parseString(backendAsMap, "bucket_container_name", nil)
			if !ok {
				err = fmt.Errorf("missing or bad bucket_container_name at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.prefix, ok = parseString(backendAsMap, "prefix", "")
			if !ok {
				err = fmt.Errorf("bad prefix at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}
			if (backendAsStructNew.prefix != "") && !strings.HasSuffix(backendAsStructNew.prefix, "/") {
				err = fmt.Errorf("bad prefix at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.traceLevel, ok = parseUint64(backendAsMap, "trace_level", uint64(0))
			if !ok {
				err = fmt.Errorf("bad trace_level at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			backendAsStructNew.backendType, ok = parseString(backendAsMap, "backend_type", nil)
			if !ok {
				err = fmt.Errorf("missing or bad bucket_container_name at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			switch backendAsStructNew.backendType {
			case "Azure":
				err = fmt.Errorf("backends[%v (\"%s\")] specified currently unsupported backend_type \"%s\"", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName, backendAsStructNew.backendType)
				return
			case "GCP":
				err = fmt.Errorf("backends[%v (\"%s\")] specified currently unsupported backend_type \"%s\"", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName, backendAsStructNew.backendType)
				return
			case "OCI":
				err = fmt.Errorf("backends[%v (\"%s\")] specified currently unsupported backend_type \"%s\"", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName, backendAsStructNew.backendType)
				return
			case "RAM":
				backendConfigRAMAsInterface, ok = backendAsMap["RAM"]
				if ok {
					backendConfigRAMAsMap, ok = backendConfigRAMAsInterface.(map[string]interface{})
					if !ok {
						err = fmt.Errorf("bad RAM section at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigRAMAsStruct = &backendConfigRAMStruct{}

					backendConfigRAMAsStruct.maxTotalObjects, ok = parseUint64(backendConfigRAMAsMap, "max_total_objects", defaultRAMMaxTotalObjects)
					if !ok {
						err = fmt.Errorf("bad RAM.max_total_objects at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigRAMAsStruct.maxTotalObjectSpace, ok = parseUint64(backendConfigRAMAsMap, "max_total_object_space", defaultRAMMaxTotalObjectSpace)
					if !ok {
						err = fmt.Errorf("bad RAM.max_total_object_space at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigRAMAsStruct.maxDirectoryPageSize, ok = parseUint64(backendConfigRAMAsMap, "max_directory_page_size", defaultRAMMaxDirectoryPageSize)
					if !ok {
						err = fmt.Errorf("bad RAM.max_directory_page_size at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}
				} else {
					backendConfigRAMAsStruct = &backendConfigRAMStruct{
						maxTotalObjects:      defaultRAMMaxTotalObjects,
						maxTotalObjectSpace:  defaultRAMMaxTotalObjectSpace,
						maxDirectoryPageSize: defaultRAMMaxDirectoryPageSize,
					}
				}

				backendAsStructNew.backendTypeSpecifics = backendConfigRAMAsStruct
			case "S3":
				backendConfigS3AsInterface, ok = backendAsMap["S3"]
				if !ok {
					err = fmt.Errorf("missing or bad S3 section at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsMap, ok = backendConfigS3AsInterface.(map[string]interface{})
				if !ok {
					err = fmt.Errorf("bad S3 section at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct = &backendConfigS3Struct{}

				backendConfigS3AsStruct.configCredentialsProfile, ok = parseString(backendConfigS3AsMap, "config_credentials_profile", "default")
				if !ok {
					err = fmt.Errorf("bad S3.config_credentials_profile at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.useConfigEnv, ok = parseBool(backendConfigS3AsMap, "use_config_env", false)
				if !ok {
					err = fmt.Errorf("bad S3.use_config_env at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				if backendConfigS3AsStruct.useConfigEnv {
					backendConfigS3AsStruct.configFilePath, ok = parseString(backendConfigS3AsMap, "config_file_path", "${AWS_CONFIG_FILE:-${HOME}/.aws/config}")
					if !ok {
						err = fmt.Errorf("bad S3.config_file_path at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigS3AsStruct.region = ""
					backendConfigS3AsStruct.endpoint = ""
				} else {
					backendConfigS3AsStruct.configFilePath = ""

					backendConfigS3AsStruct.region, ok = parseString(backendConfigS3AsMap, "region", "${AWS_REGION:-us-east-1}")
					if !ok {
						err = fmt.Errorf("bad S3.region at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigS3AsStruct.endpoint, ok = parseString(backendConfigS3AsMap, "endpoint", "${AWS_ENDPOINT}")
					if !ok {
						err = fmt.Errorf("bad S3.endpoint at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}
				}

				backendConfigS3AsStruct.useCredentialsEnv, ok = parseBool(backendConfigS3AsMap, "use_credentials_env", false)
				if !ok {
					err = fmt.Errorf("bad S3.use_credentials_env at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				if backendConfigS3AsStruct.useCredentialsEnv {
					backendConfigS3AsStruct.credentialsFilePath, ok = parseString(backendConfigS3AsMap, "credentials_file_path", "${AWS_SHARED_CREDENTIALS_FILE:-${HOME}/.aws/credentials}")
					if !ok {
						err = fmt.Errorf("bad S3.credentials_file_path at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigS3AsStruct.accessKeyID = ""
					backendConfigS3AsStruct.secretAccessKey = ""
				} else {
					backendConfigS3AsStruct.credentialsFilePath = ""

					backendConfigS3AsStruct.accessKeyID, ok = parseString(backendConfigS3AsMap, "access_key_id", "${AWS_ACCESS_KEY_ID}")
					if !ok {
						err = fmt.Errorf("bad S3.access_key_id at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}
					if backendConfigS3AsStruct.accessKeyID == "" {
						err = fmt.Errorf("empty S3.access_key_id at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}

					backendConfigS3AsStruct.secretAccessKey, ok = parseString(backendConfigS3AsMap, "secret_access_key", "${AWS_SECRET_ACCESS_KEY}")
					if !ok {
						err = fmt.Errorf("bad S3.secret_access_key at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}
					if backendConfigS3AsStruct.secretAccessKey == "" {
						err = fmt.Errorf("empty S3.secret_access_key at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
						return
					}
				}

				backendConfigS3AsStruct.skipTLSCertificateVerify, ok = parseBool(backendConfigS3AsMap, "skip_tls_certificate_verify", true)
				if !ok {
					err = fmt.Errorf("bad S3.skip_tls_certificate_verify at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.virtualHostedStyleRequest, ok = parseBool(backendConfigS3AsMap, "virtual_hosted_style_request", false)
				if !ok {
					err = fmt.Errorf("bad S3.virtual_hosted_style_request at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.unsignedPayload, ok = parseBool(backendConfigS3AsMap, "unsigned_payload", false)
				if !ok {
					err = fmt.Errorf("bad S3.unsigned_payload at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.retryBaseDelay, ok = parseMilliseconds(backendConfigS3AsMap, "retry_base_delay", 10*time.Millisecond)
				if !ok {
					err = fmt.Errorf("bad S3.retry_base_delay at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.retryNextDelayMultiplier, ok = parseFloat64(backendConfigS3AsMap, "retry_next_delay_multiplier", float64(2.0))
				if !ok || (backendConfigS3AsStruct.retryNextDelayMultiplier < float64(1.0)) {
					err = fmt.Errorf("bad S3.retry_next_delay_multiplier at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.retryMaxDelay, ok = parseMilliseconds(backendConfigS3AsMap, "retry_max_delay", 2000*time.Millisecond)
				if !ok {
					err = fmt.Errorf("bad S3.retry_max_delay at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
					return
				}

				backendConfigS3AsStruct.retryDelay = make([]time.Duration, 0)

				if backendConfigS3AsStruct.retryBaseDelay != time.Duration(0) {
					nextRetryDelay = backendConfigS3AsStruct.retryBaseDelay

					for nextRetryDelay <= backendConfigS3AsStruct.retryMaxDelay {
						backendConfigS3AsStruct.retryDelay = append(backendConfigS3AsStruct.retryDelay, nextRetryDelay)
						nextRetryDelay = time.Duration(float64(nextRetryDelay) * backendConfigS3AsStruct.retryNextDelayMultiplier)
					}
				}

				backendAsStructNew.backendTypeSpecifics = backendConfigS3AsStruct
			default:
				err = fmt.Errorf("backends[%v (\"%s\")] specified unsupported backend_type \"%s\"", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName, backendAsStructNew.backendType)
				return
			}

			_, ok = config.backends[backendAsStructNew.dirName]
			if ok {
				err = fmt.Errorf("duplicate backend at backends[%v (\"%s\")]", backendsAsInterfaceSliceIndex, backendAsStructNew.dirName)
				return
			}

			config.backends[backendAsStructNew.dirName] = backendAsStructNew
		}
	}

	if globals.config == nil {
		// Move all (local) config.backends to globals.backendsToMount

		for dirName, backendAsStructNew = range config.backends {
			delete(config.backends, dirName)
			globals.backendsToMount[dirName] = backendAsStructNew
		}

		// Finally, just set globals.config to be our (local) config

		globals.config = config
		globals.configFileMap = configFileMap // Store for msc_config attribute provider
	} else {
		// Validate that no global config changes were made

		if globals.config.msfsVersion != config.msfsVersion {
			err = errors.New("cannot change msfs_version via SIGHUP")
			return
		}

		if globals.config.mountName != config.mountName {
			err = errors.New("cannot change mountname via SIGHUP")
			return
		}

		if globals.config.mountPoint != config.mountPoint {
			err = errors.New("cannot change mountpoint via SIGHUP")
			return
		}

		if globals.config.uid != config.uid {
			err = errors.New("cannot change uid via SIGHUP")
			return
		}

		if globals.config.gid != config.gid {
			err = errors.New("cannot change gid via SIGHUP")
			return
		}

		if globals.config.dirPerm != config.dirPerm {
			err = errors.New("cannot change dir_perm via SIGHUP")
			return
		}

		if globals.config.allowOther != config.allowOther {
			err = errors.New("cannot change allow_other via SIGHUP")
			return
		}

		if globals.config.maxWrite != config.maxWrite {
			err = errors.New("cannot change max_write via SIGHUP")
			return
		}

		if globals.config.entryAttrTTL != config.entryAttrTTL {
			err = errors.New("cannot change entry_attr_ttl via SIGHUP")
			return
		}

		if globals.config.evictableInodeTTL != config.evictableInodeTTL {
			err = errors.New("cannot change evictable_inode_ttl via SIGHUP")
			return
		}

		if globals.config.cacheLineSize != config.cacheLineSize {
			err = errors.New("cannot change cache_line_size via SIGHUP")
			return
		}

		if globals.config.cacheLines != config.cacheLines {
			err = errors.New("cannot change cache_lines via SIGHUP")
			return
		}

		if globals.config.dirtyCacheLinesFlushTrigger != config.dirtyCacheLinesFlushTrigger {
			err = errors.New("cannot change dirty_cache_lines_flush_trigger via SIGHUP")
			return
		}

		if globals.config.dirtyCacheLinesMax != config.dirtyCacheLinesMax {
			err = errors.New("cannot change dirty_cache_lines_max via SIGHUP")
			return
		}

		if globals.config.autoSIGHUPInterval != config.autoSIGHUPInterval {
			err = errors.New("cannot change auto_sighup_interval via SIGHUP")
			return
		}

		// Verify that all backends common to our (local) config.backends and globals.backends contain no changes

		for dirName, backendAsStructOld = range globals.config.backends {
			backendAsStructNew, ok = config.backends[dirName]
			if ok {
				if backendAsStructOld.readOnly != backendAsStructNew.readOnly {
					err = fmt.Errorf("cannot change readonly in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.flushOnClose != backendAsStructNew.flushOnClose {
					err = fmt.Errorf("cannot change flush_on_close in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.uid != backendAsStructNew.uid {
					err = fmt.Errorf("cannot change uid in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.gid != backendAsStructNew.gid {
					err = fmt.Errorf("cannot change gid in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.dirPerm != backendAsStructNew.dirPerm {
					err = fmt.Errorf("cannot change dir_perm in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.filePerm != backendAsStructNew.filePerm {
					err = fmt.Errorf("cannot change file_perm in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.directoryPageSize != backendAsStructNew.directoryPageSize {
					err = fmt.Errorf("cannot change directory_page_size in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.multiPartCacheLineThreshold != backendAsStructNew.multiPartCacheLineThreshold {
					err = fmt.Errorf("cannot change multipart_cache_line_threshold in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.uploadPartCacheLines != backendAsStructNew.uploadPartCacheLines {
					err = fmt.Errorf("cannot change upload_part_cache_lines in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.uploadPartConcurrency != backendAsStructNew.uploadPartConcurrency {
					err = fmt.Errorf("cannot change upload_part_concurrency in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.bucketContainerName != backendAsStructNew.bucketContainerName {
					err = fmt.Errorf("cannot change bucket_container_name in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.prefix != backendAsStructNew.prefix {
					err = fmt.Errorf("cannot change prefix in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.traceLevel != backendAsStructNew.traceLevel {
					err = fmt.Errorf("cannot change trace_level in backends[\"%s\"]", dirName)
					return
				}

				if backendAsStructOld.backendType != backendAsStructNew.backendType {
					err = fmt.Errorf("cannot change backend_type in backends[\"%s\"]", dirName)
					return
				}

				switch backendAsStructOld.backendType {
				case "Azure": // not currently supported
					err = fmt.Errorf("logic error comparing backend_type specifics in backends[\"%s\"] - backend_type == \"Azure\"", dirName)
					return
				case "GCP": // not currently supported
					err = fmt.Errorf("logic error comparing backend_type specifics in backends[\"%s\"] - backend_type == \"GCP\"", dirName)
					return
				case "OCI": // not currently supported
					err = fmt.Errorf("logic error comparing backend_type specifics in backends[\"%s\"] - backend_type == \"OCI\"", dirName)
					return
				case "RAM":
					if backendAsStructOld.backendTypeSpecifics.(*backendConfigRAMStruct).maxTotalObjects != backendAsStructNew.backendTypeSpecifics.(*backendConfigRAMStruct).maxTotalObjects {
						err = fmt.Errorf("cannot change RAM.max_total_objects in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigRAMStruct).maxTotalObjectSpace != backendAsStructNew.backendTypeSpecifics.(*backendConfigRAMStruct).maxTotalObjectSpace {
						err = fmt.Errorf("cannot change RAM.max_total_object_space in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigRAMStruct).maxDirectoryPageSize != backendAsStructNew.backendTypeSpecifics.(*backendConfigRAMStruct).maxDirectoryPageSize {
						err = fmt.Errorf("cannot change RAM.max_directory_page_size in backends[\"%s\"]", dirName)
						return
					}
				case "S3":
					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).configCredentialsProfile != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).configCredentialsProfile {
						err = fmt.Errorf("cannot change S3.config_credentials_profile in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).useConfigEnv != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).useConfigEnv {
						err = fmt.Errorf("cannot change S3.use_config_env in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).configFilePath != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).configFilePath {
						err = fmt.Errorf("cannot change S3.config_file_path in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).region != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).region {
						err = fmt.Errorf("cannot change S3.region in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).endpoint != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).endpoint {
						err = fmt.Errorf("cannot change S3.endpoint in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).useCredentialsEnv != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).useCredentialsEnv {
						err = fmt.Errorf("cannot change S3.use_credentials_env in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).credentialsFilePath != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).credentialsFilePath {
						err = fmt.Errorf("cannot change S3.credentials_file_path in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).accessKeyID != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).accessKeyID {
						err = fmt.Errorf("cannot change S3.access_key_id in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).secretAccessKey != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).secretAccessKey {
						err = fmt.Errorf("cannot change S3.secret_access_key in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).skipTLSCertificateVerify != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).skipTLSCertificateVerify {
						err = fmt.Errorf("cannot change S3.skip_tls_certificate_verify in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).virtualHostedStyleRequest != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).virtualHostedStyleRequest {
						err = fmt.Errorf("cannot change S3.virtual_hosted_style_request in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).unsignedPayload != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).unsignedPayload {
						err = fmt.Errorf("cannot change S3.unsigned_payload in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).retryBaseDelay != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).retryBaseDelay {
						err = fmt.Errorf("cannot change S3.retry_base_delay in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).retryNextDelayMultiplier != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).retryNextDelayMultiplier {
						err = fmt.Errorf("cannot change S3.retry_next_delay_multiplier in backends[\"%s\"]", dirName)
						return
					}

					if backendAsStructOld.backendTypeSpecifics.(*backendConfigS3Struct).retryMaxDelay != backendAsStructNew.backendTypeSpecifics.(*backendConfigS3Struct).retryMaxDelay {
						err = fmt.Errorf("cannot change S3.retry_max_delay in backends[\"%s\"]", dirName)
						return
					}
				default:
					err = fmt.Errorf("logic error comparing backend_type specifics in backends[\"%s\"] - backend_type \"%s\" unrecognized", dirName, backendAsStructOld.backendType)
					return
				}
			}
		}

		// Clone references to all globals.backends backends missing from (local) config.backends to globals.backendsToUnmount

		for dirName, backendAsStructOld = range globals.config.backends {
			_, ok = config.backends[dirName]
			if !ok {
				globals.backendsToUnmount[dirName] = backendAsStructOld
			}
		}

		// Clone references to all (local) config.backends missing from globals.backends to globals.backendsToMount

		for dirName, backendAsStructNew = range config.backends {
			_, ok = globals.config.backends[dirName]
			if !ok {
				globals.backendsToMount[dirName] = backendAsStructNew
			}
		}
	}

	// All done

	err = nil
	return
}
