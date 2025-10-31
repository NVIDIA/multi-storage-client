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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MSCPMetricsDiperiodic holds all metric instruments for MSCP using the diperiodic pattern.
// Matches MSC Python's approach exactly:
// - Gauges (LastValue aggregation) for raw samples -> distributions
// - Counters (Sum aggregation) for sums
//
// ATTRIBUTE COLLECTION DIFFERENCE (Python vs Go):
//
// Python MSC: Attribute providers are collected PER-METRIC (on every metric call)
//   - Reason: Python MSC is a LIBRARY used in multi-process applications
//   - Applications use multiprocessing.Process() to spawn worker processes (sync.py:361-374)
//   - ThreadAttributesProvider: thread ID changes per thread
//   - ProcessAttributesProvider: process ID changes after fork()
//   - Calls socket.gethostname(), threading.current_thread() on every metric
//   - See: src/multistorageclient/providers/base.py line 202
//
// Go MSCP: Attribute providers are collected ONCE (at startup) and stored in Resource
//   - Reason: MSCP is a SINGLE-PROCESS DAEMON (not a library)
//   - Multiple client processes access MSCP via FUSE, but MSCP itself never forks
//   - Uses goroutines for concurrency (all within same process - PID never changes)
//   - Goroutines share same PID, hostname, environment throughout MSCP daemon lifetime
//   - Resource attributes are Go/OTel best practice for process-level attributes
//   - More efficient: no repeated system calls or allocations per metric
//   - See: posix/fuse/mscp/telemetry/setup.go lines 102-117
//
// ARCHITECTURAL DIFFERENCE:
//
//	Python MSC: Library → Used in multi-process apps → PID changes → Per-metric collection
//	Go MSCP:    Daemon  → Single-process lifetime    → PID fixed   → Startup collection
//
// Both approaches produce IDENTICAL OTLP output for their respective use cases.
type MSCPMetricsDiperiodic struct {
	meter metric.Meter

	// Attributes from attribute providers (collected once at startup)
	// These are added to EVERY metric recording (matching Python behavior)
	// Python collects these per-metric, but MSCP can collect once since it's a single-process daemon
	baseAttributes []attribute.KeyValue

	// Gauges (using Gauge instrument with LastValue aggregation)
	// Go OTel has synchronous Gauge types (Float64Gauge, Int64Gauge)
	// These automatically use LastValue aggregation (matching Python's _Gauge)
	latencyGauge  metric.Float64Gauge // multistorageclient.latency (unit: s)
	dataSizeGauge metric.Int64Gauge   // multistorageclient.data_size (unit: By)
	dataRateGauge metric.Float64Gauge // multistorageclient.data_rate (unit: By/s)

	// Counters (for sums - decomposable aggregations)
	requestSumCounter  metric.Int64Counter // multistorageclient.request.sum
	responseSumCounter metric.Int64Counter // multistorageclient.response.sum
	dataSizeSumCounter metric.Int64Counter // multistorageclient.data_size.sum
}

// NewMSCPMetricsDiperiodic creates all MSCP metric instruments using diperiodic pattern.
// serviceName is typically "msc-posix"
// baseAttributes are attributes from attribute providers (msc.ppp, msc.cluster, etc.) added to every metric
//
// Note: We use Float64Counter/Int64Counter for gauges because Go doesn't have synchronous Gauge.
// The View configuration applies LastValue aggregation to these, making them behave exactly like
// Python's _Gauge (which also uses LastValue aggregation internally).
func NewMSCPMetricsDiperiodic(serviceName string, baseAttributes []attribute.KeyValue) (MSCPMetricsDiperiodic, error) {
	meter := otel.Meter(serviceName)

	// Gauges (using native Gauge instruments)
	// Go OTel has synchronous Gauge types that automatically use LastValue aggregation
	// This matches Python's _Gauge which also uses LastValue internally
	latencyGauge, err := meter.Float64Gauge(
		"multistorageclient.latency",
		metric.WithDescription("Latency per individual operation (gauge with LastValue)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	dataSizeGauge, err := meter.Int64Gauge(
		"multistorageclient.data_size",
		metric.WithDescription("Data size per individual operation (gauge with LastValue)"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	dataRateGauge, err := meter.Float64Gauge(
		"multistorageclient.data_rate",
		metric.WithDescription("Data rate per individual operation (gauge with LastValue)"),
		metric.WithUnit("By/s"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	// Counters (for sums) - these use default Sum aggregation
	requestSumCounter, err := meter.Int64Counter(
		"multistorageclient.request.sum",
		metric.WithDescription("Total number of requests"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	responseSumCounter, err := meter.Int64Counter(
		"multistorageclient.response.sum",
		metric.WithDescription("Total number of responses"),
		metric.WithUnit("{response}"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	dataSizeSumCounter, err := meter.Int64Counter(
		"multistorageclient.data_size.sum",
		metric.WithDescription("Total data size across all operations"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return MSCPMetricsDiperiodic{}, err
	}

	return MSCPMetricsDiperiodic{
		meter:              meter,
		baseAttributes:     baseAttributes,
		latencyGauge:       latencyGauge,
		dataSizeGauge:      dataSizeGauge,
		dataRateGauge:      dataRateGauge,
		requestSumCounter:  requestSumCounter,
		responseSumCounter: responseSumCounter,
		dataSizeSumCounter: dataSizeSumCounter,
	}, nil
}

// RecordBackendRequest records the request counter at the START of a backend operation.
// Matches Python's behavior: request.sum is recorded BEFORE the operation (line 209).
// This should be called immediately at function start, NOT in defer.
func (m *MSCPMetricsDiperiodic) RecordBackendRequest(ctx context.Context, operation string, version string, backend string) {
	// Build attribute slice - merging base attributes with operation-specific ones
	// Python base.py lines 201-206: collect_attributes() + VERSION, PROVIDER, OPERATION
	allAttrs := make([]attribute.KeyValue, 0, len(m.baseAttributes)+3)
	allAttrs = append(allAttrs, m.baseAttributes...)
	allAttrs = append(allAttrs,
		attribute.String("multistorageclient.version", version),
		attribute.String("multistorageclient.provider", backend),
		attribute.String("multistorageclient.operation", operation),
	)

	// Record request counter (matches Python line 209)
	m.requestSumCounter.Add(ctx, 1, metric.WithAttributes(allAttrs...))
}

// RecordBackendOperation records metrics for a backend operation using diperiodic pattern.
// Matches Python's BaseStorageProvider._emit_metrics()
// Note: Does not accept additional attributes to avoid high cardinality issues in metric backends.
func (m *MSCPMetricsDiperiodic) RecordBackendOperation(ctx context.Context, operation string, version string, backend string, duration time.Duration, success bool, bytesTransferred int64) {
	status := "success"
	if !success {
		// Go errors don't have nice class names like Python (e.g. TimeoutError)
		// Use "error.Go" to indicate this is a Go error (matches Python pattern: "error.{ErrorType}")
		status = "error.Go"
	}

	// Build attribute slice - merging base attributes with operation-specific ones
	// Python base.py lines 201-206: collect_attributes() + VERSION, PROVIDER, OPERATION (and STATUS added after operation)
	allAttrs := make([]attribute.KeyValue, 0, len(m.baseAttributes)+4)
	allAttrs = append(allAttrs, m.baseAttributes...)
	allAttrs = append(allAttrs,
		attribute.String("multistorageclient.version", version),
		attribute.String("multistorageclient.provider", backend),
		attribute.String("multistorageclient.operation", operation),
		attribute.String("multistorageclient.status", status),
	)

	// Record response counter first (always, matches Python line 239-241)
	// Python: "Always record responses. The only metrics we skip on failure are data size ones."
	m.responseSumCounter.Add(ctx, 1, metric.WithAttributes(allAttrs...))

	// Record latency (gauge - individual sample)
	m.latencyGauge.Record(ctx, duration.Seconds(), metric.WithAttributes(allAttrs...))

	// Record data size metrics (only on success, matches Python line 239-241)
	if success && bytesTransferred > 0 {
		m.dataSizeGauge.Record(ctx, bytesTransferred, metric.WithAttributes(allAttrs...))

		// Calculate and record data rate
		if duration.Seconds() > 0 {
			dataRate := float64(bytesTransferred) / duration.Seconds()
			m.dataRateGauge.Record(ctx, dataRate, metric.WithAttributes(allAttrs...))
		}

		// Record data size sum (counter)
		m.dataSizeSumCounter.Add(ctx, bytesTransferred, metric.WithAttributes(allAttrs...))
	}
}
