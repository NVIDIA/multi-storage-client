// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"time"

	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/attributes"
	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/auth"
	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/metrics/exporters"
	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/metrics/readers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// MetricsConfig holds configuration for OTLP metrics export.
// Matches Python MSC's reader options exactly.
type MetricsConfig struct {
	Enabled               bool
	OTLPEndpoint          string                          // e.g. "otel-collector:4318" (HTTP/OTLP)
	CollectIntervalMillis uint64                          // Collection interval in milliseconds, default: 1000 (1 second)
	CollectTimeoutMillis  uint64                          // Collection timeout in milliseconds, default: 10000 (10 seconds)
	ExportIntervalMillis  uint64                          // Export interval in milliseconds, default: 60000 (60 seconds)
	ExportTimeoutMillis   uint64                          // Export timeout in milliseconds, default: 30000 (30 seconds)
	ServiceName           string                          //
	Insecure              bool                            // If true, use insecure connection (no TLS)
	AzureAuth             *auth.Config                    // Optional: Azure MSAL auth config for _otlp_msal exporter
	AttributeProviders    []attributes.AttributesProvider // Attribute providers to add to resource (matches Python)
}

// SetupMetricsDiperiodic initializes the OTLP metrics exporter with diperiodic pattern.
// This matches MSC Python's DiperiodicExportingMetricReader exactly:
// - Separate collection (default 1s) and export (default 60s) intervals
// - Double buffering to avoid blocking
// - LastValue aggregation for gauges
// - Sum aggregation for counters
//
// Returns the MeterProvider and collected attributes from attribute providers.
// The attributes should be added to each metric recording (matching Python behavior).
//
// Matches Python: telemetry/__init__.py:meter_provider()
func SetupMetricsDiperiodic(config MetricsConfig) (*sdkmetric.MeterProvider, []attribute.KeyValue, error) {
	ctx := context.Background()

	// Create exporter based on auth configuration
	var exporter sdkmetric.Exporter
	var err error

	if config.AzureAuth != nil {
		// Create OTLP exporter with MSAL auth (_otlp_msal)
		exporter, err = exporters.NewOTLPMSALExporter(*config.AzureAuth, config.OTLPEndpoint)
		if err != nil {
			return nil, nil, err
		}
	} else {
		// Create standard OTLP/HTTP exporter (otlp)
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(config.OTLPEndpoint),
		}
		if config.Insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}

		exporter, err = otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create diperiodic reader with configurable intervals (matches Python reader options)
	collectInterval := time.Duration(config.CollectIntervalMillis) * time.Millisecond
	if collectInterval <= 0 {
		collectInterval = time.Duration(readers.DefaultCollectIntervalMillis) * time.Millisecond
	}

	collectTimeout := time.Duration(config.CollectTimeoutMillis) * time.Millisecond
	if collectTimeout <= 0 {
		collectTimeout = time.Duration(readers.DefaultCollectTimeoutMillis) * time.Millisecond
	}

	exportInterval := time.Duration(config.ExportIntervalMillis) * time.Millisecond
	if exportInterval <= 0 {
		exportInterval = time.Duration(readers.DefaultExportIntervalMillis) * time.Millisecond
	}

	exportTimeout := time.Duration(config.ExportTimeoutMillis) * time.Millisecond
	if exportTimeout <= 0 {
		exportTimeout = time.Duration(readers.DefaultExportTimeoutMillis) * time.Millisecond
	}

	reader := readers.NewDiperiodicReader(
		exporter,
		readers.WithCollectInterval(collectInterval),
		readers.WithCollectTimeout(collectTimeout),
		readers.WithExportInterval(exportInterval),
		readers.WithExportTimeout(exportTimeout),
	)

	// Collect attributes from attribute providers (matches Python: collect_attributes())
	var resourceAttrs []attribute.KeyValue
	var metricAttrs []attribute.KeyValue // Attributes to add to each metric recording (Python behavior)

	if len(config.AttributeProviders) > 0 {
		resourceAttrs = attributes.CollectAttributes(config.AttributeProviders)
		metricAttrs = make([]attribute.KeyValue, len(resourceAttrs))
		copy(metricAttrs, resourceAttrs) // Make a copy for metric labels
	}

	// Add service name as a resource attribute
	resourceAttrs = append(resourceAttrs, semconv.ServiceName(config.ServiceName))

	// Create resource with service name and collected attributes
	// Matches Python: sdk_metrics.MeterProvider(metric_readers=[reader])
	// Python adds attributes per-metric, but Go best practice is to add them to resource
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		resourceAttrs...,
	)

	// Create meter provider with diperiodic reader
	// Note: Gauge instruments automatically use LastValue aggregation
	// This matches Python's _Gauge behavior
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	// Set global meter provider
	otel.SetMeterProvider(meterProvider)

	// Return meterProvider and metricAttrs (to be added to each metric recording)
	return meterProvider, metricAttrs, nil
}
