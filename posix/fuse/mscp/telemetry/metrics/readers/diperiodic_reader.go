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

package readers

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Constants matching Python MSC defaults
const (
	DefaultCollectIntervalMillis = 1000  // 1 second
	DefaultCollectTimeoutMillis  = 10000 // 10 seconds
	DefaultExportIntervalMillis  = 60000 // 60 seconds
	DefaultExportTimeoutMillis   = 30000 // 30 seconds
)

// DiperiodicReader implements the diperiodic pattern by wrapping ManualReader.
//
// Key concept: When Gauge.Record() is called, it replaces the current value in the gauge.
// When Reader.Collect() is called, it takes a snapshot of current gauge values as data points.
//
// This reader:
// 1. Collects snapshots every collectInterval (default 1s) into collectBuffer
// 2. Exports accumulated snapshots every exportInterval (default 60s) from exportBuffer
// 3. Uses double buffering to avoid blocking during export
//
// Matches Python MSC's DiperiodicExportingMetricReader exactly.
//
// Implementation note: Go's OTel SDK's Reader interface has unexported methods that prevent
// external implementation. We wrap ManualReader (which properly implements Reader) and manage
// the timing/buffering ourselves.
type DiperiodicReader struct {
	*metric.ManualReader // Embed to inherit Reader interface
	exporter             metric.Exporter

	// Intervals
	collectInterval time.Duration
	collectTimeout  time.Duration
	exportInterval  time.Duration
	exportTimeout   time.Duration

	// Double buffering - accumulate data points across multiple collections
	collectBuffer []metricdata.ResourceMetrics
	exportBuffer  []metricdata.ResourceMetrics
	collectMu     sync.Mutex
	exportMu      sync.Mutex

	// Background goroutines - matches inode evictor pattern from fs.go
	collectTicker *time.Ticker
	exportTicker  *time.Ticker
	ctx           context.Context    // Context for goroutine cancellation
	cancelFunc    context.CancelFunc // Cancel function to stop goroutines
	wg            sync.WaitGroup     // WaitGroup to track goroutine completion
	shutdownOnce  sync.Once

	// Flushable for manual triggers
	flushChan chan chan error
}

// Option is a functional option for DiperiodicReader
type Option func(*DiperiodicReader)

// WithCollectInterval sets the collection interval
func WithCollectInterval(d time.Duration) Option {
	return func(r *DiperiodicReader) {
		r.collectInterval = d
	}
}

// WithCollectTimeout sets the collection timeout
func WithCollectTimeout(d time.Duration) Option {
	return func(r *DiperiodicReader) {
		r.collectTimeout = d
	}
}

// WithExportInterval sets the export interval
func WithExportInterval(d time.Duration) Option {
	return func(r *DiperiodicReader) {
		r.exportInterval = d
	}
}

// WithExportTimeout sets the export timeout
func WithExportTimeout(d time.Duration) Option {
	return func(r *DiperiodicReader) {
		r.exportTimeout = d
	}
}

// NewDiperiodicReader creates a new diperiodic metric reader.
//
// The reader runs two background goroutines:
// - Collect goroutine: Calls Collect() every collectInterval to snapshot current metrics
// - Export goroutine: Exports accumulated snapshots every exportInterval
//
// This allows collecting frequent snapshots (1s) while exporting less frequently (60s),
// reducing network traffic while preserving temporal resolution of the data.
//
// Returns the underlying ManualReader which should be registered with MeterProvider.
func NewDiperiodicReader(exporter metric.Exporter, opts ...Option) metric.Reader {
	// Create context for goroutine lifecycle management (matches inode evictor pattern)
	ctx, cancelFunc := context.WithCancel(context.Background())

	r := &DiperiodicReader{
		ManualReader:    metric.NewManualReader(), // Embed SDK's ManualReader
		exporter:        exporter,
		collectInterval: DefaultCollectIntervalMillis * time.Millisecond,
		collectTimeout:  DefaultCollectTimeoutMillis * time.Millisecond,
		exportInterval:  DefaultExportIntervalMillis * time.Millisecond,
		exportTimeout:   DefaultExportTimeoutMillis * time.Millisecond,
		collectBuffer:   make([]metricdata.ResourceMetrics, 0),
		exportBuffer:    make([]metricdata.ResourceMetrics, 0),
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		flushChan:       make(chan chan error, 1),
	}

	// Apply options
	for _, opt := range opts {
		opt(r)
	}

	// Start background goroutines using WaitGroup (matches inode evictor pattern)
	r.collectTicker = time.NewTicker(r.collectInterval)
	r.exportTicker = time.NewTicker(r.exportInterval)

	r.wg.Add(2) // Track both goroutines
	go r.collectDaemon()
	go r.exportDaemon()

	// Return DiperiodicReader itself - it embeds ManualReader so it implements metric.Reader
	// When SDK shuts down, it will call our Shutdown() method to cleanup goroutines
	return r
}

// ForceFlush flushes all pending metrics immediately.
// This collects current metrics and exports everything that's buffered.
func (r *DiperiodicReader) ForceFlush(ctx context.Context) error {
	// Trigger immediate collect and export via the export daemon
	errCh := make(chan error, 1)
	select {
	case r.flushChan <- errCh:
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	// Also flush the exporter to ensure data is sent
	return r.exporter.ForceFlush(ctx)
}

// Shutdown stops the reader and flushes remaining metrics.
// This overrides the embedded ManualReader's Shutdown to cleanup our goroutines.
// Follows the same pattern as drainFS() in fs.go for inode evictor shutdown.
func (r *DiperiodicReader) Shutdown(ctx context.Context) error {
	var err error

	r.shutdownOnce.Do(func() {
		// Cancel context to signal goroutines to stop (matches inodeEvictorCancelFunc())
		r.cancelFunc()

		// Wait for goroutines to complete (matches inodeEvictorWaitGroup.Wait())
		r.wg.Wait()

		// Stop tickers after goroutines have stopped
		if r.collectTicker != nil {
			r.collectTicker.Stop()
		}
		if r.exportTicker != nil {
			r.exportTicker.Stop()
		}

		// Final export
		r.doExport(ctx)

		// Shutdown embedded ManualReader
		if err2 := r.ManualReader.Shutdown(ctx); err2 != nil {
			err = err2
		}

		// Shutdown exporter
		if err2 := r.exporter.Shutdown(ctx); err2 != nil && err == nil {
			err = err2
		}
	})

	return err
}

// collectDaemon runs in a background goroutine and triggers metric collection
// at the configured interval. Follows the same pattern as inodeEvictor() in fs.go.
func (r *DiperiodicReader) collectDaemon() {
	defer r.wg.Done() // Signal completion when goroutine exits

	for {
		select {
		case <-r.collectTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.collectTimeout)
			r.doCollect(ctx)
			cancel()

		case <-r.ctx.Done():
			// Context cancelled - stop the ticker and return (matches inode evictor pattern)
			return
		}
	}
}

// exportDaemon runs in a background goroutine and triggers metric export
// at the configured interval. Follows the same pattern as inodeEvictor() in fs.go.
func (r *DiperiodicReader) exportDaemon() {
	defer r.wg.Done() // Signal completion when goroutine exits

	for {
		select {
		case <-r.exportTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.exportTimeout)
			r.doExport(ctx)
			cancel()

		case errCh := <-r.flushChan:
			ctx, cancel := context.WithTimeout(context.Background(), r.exportTimeout)
			r.doCollect(ctx)
			err := r.doExport(ctx)
			cancel()
			errCh <- err

		case <-r.ctx.Done():
			// Context cancelled - stop the ticker and return (matches inode evictor pattern)
			return
		}
	}
}

// doCollect performs a single collection iteration and appends the result to collectBuffer.
func (r *DiperiodicReader) doCollect(ctx context.Context) error {
	// Collect current snapshot using the embedded ManualReader
	var rm metricdata.ResourceMetrics
	if err := r.ManualReader.Collect(ctx, &rm); err != nil {
		otel.Handle(err)
		return err
	}

	// Add snapshot to collect buffer
	r.collectMu.Lock()
	defer r.collectMu.Unlock()

	// Append this snapshot to our buffer
	// Each call to Collect() represents a snapshot of current gauge values
	r.collectBuffer = append(r.collectBuffer, rm)

	return nil
}

// doExport performs a single export iteration with buffer rotation.
// Matches Python's _export_iteration() method.
//
// Locking pattern (matches Python):
// 1. Acquire exportMu first and hold for entire function (prevents concurrent exports)
// 2. Briefly acquire collectMu only during buffer rotation (allows collections to continue)
func (r *DiperiodicReader) doExport(ctx context.Context) error {
	// Acquire export lock for entire function (matches Python: self._export_buffer_lock)
	r.exportMu.Lock()
	defer r.exportMu.Unlock()

	// Briefly acquire collect lock only for buffer rotation (matches Python pattern)
	r.collectMu.Lock()
	r.exportBuffer = r.collectBuffer
	r.collectBuffer = make([]metricdata.ResourceMetrics, 0)
	r.collectMu.Unlock()

	// Export all accumulated snapshots (protected by exportMu held above)
	if len(r.exportBuffer) == 0 {
		return nil
	}

	// Merge all ResourceMetrics into one for export
	merged := r.exportBuffer[0]
	for i := 1; i < len(r.exportBuffer); i++ {
		merged.ScopeMetrics = append(merged.ScopeMetrics, r.exportBuffer[i].ScopeMetrics...)
	}

	// Export the merged data
	err := r.exporter.Export(ctx, &merged)
	if err != nil {
		otel.Handle(err)
	}

	// Clear export buffer after export (still protected by defer unlock above)
	r.exportBuffer = make([]metricdata.ResourceMetrics, 0)

	return err
}
