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

package exporters

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/auth"
	"github.com/hashicorp/go-retryablehttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	// HTTP retry configuration - matches Python _OTLPMSALMetricExporter
	maxRetries          = 5    // _MAX_RETRIES = 5
	retryWaitMin        = 500  // milliseconds (backoff_factor = 0.5 → first retry 0.5s)
	retryWaitMax        = 8000 // milliseconds (max backoff: 0.5 * 2^4 = 8s)
	httpClientTimeout   = 60   // seconds (overall timeout for HTTP requests)
	tokenAcquireTimeout = 30   // seconds (timeout for acquiring access token)
)

// `NewOTLPMSALExporter` creates an OTLP exporter with MSAL auth and automatic retries.
// Matches Python: `_OTLPMSALMetricExporter.__init__(auth, exporter)`
//
// Uses go-retryablehttp library (similar to Python's `requests.adapters.HTTPAdapter`):
//   - RetryMax: 5 (matches Python _MAX_RETRIES)
//   - RetryWaitMin: 500ms (matches Python backoff_factor=0.5, first retry = 0.5s)
//   - RetryWaitMax: 8s (matches Python max backoff after 5 retries)
//
// Returns a standard `sdkmetric.Exporter` with MSAL authentication and retry logic built-in.
func NewOTLPMSALExporter(authConfig auth.Config, endpoint string) (sdkmetric.Exporter, error) {
	// Create Azure token provider
	tokenProvider, err := auth.NewAzureAccessTokenProvider(authConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create token provider: %w", err)
	}

	// Create retryable HTTP client (matches Python's requests_adapters.Retry)
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = maxRetries
	retryClient.RetryWaitMin = retryWaitMin * time.Millisecond
	retryClient.RetryWaitMax = retryWaitMax * time.Millisecond
	retryClient.HTTPClient.Timeout = httpClientTimeout * time.Second
	retryClient.Logger = nil // Disable retryablehttp's verbose logging

	// Wrap transport with MSAL auth (adds Bearer token to each request)
	retryClient.HTTPClient.Transport = &msalRoundTripper{
		base:          retryClient.HTTPClient.Transport,
		tokenProvider: tokenProvider,
	}

	// Create OTLP HTTP exporter with retryable client
	// The library handles retries automatically with exponential backoff
	return otlpmetrichttp.New(
		context.Background(),
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithURLPath("/v1/metrics"),
		otlpmetrichttp.WithHTTPClient(retryClient.StandardClient()), // Convert to *http.Client
	)
}

// msalRoundTripper adds Bearer tokens to HTTP requests.
// Matches Python: AccessTokenHTTPAdapter.send()
//
// Retry logic is handled by go-retryablehttp library (wrapping this transport),
// just like Python's requests.adapters.HTTPAdapter wraps the session.
type msalRoundTripper struct {
	base          http.RoundTripper
	tokenProvider *auth.AzureAccessTokenProvider
}

// `RoundTrip` implements `http.RoundTripper` interface.
// Matches Python's `AccessTokenHTTPAdapter.send()` method (lines 54-61).
func (t *msalRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Get token with timeout (Python doesn't explicitly set timeout, but we add it for safety)
	ctx, cancel := context.WithTimeout(req.Context(), tokenAcquireTimeout*time.Second)
	defer cancel()

	// Clone request first (before token acquisition)
	reqClone := req.Clone(req.Context())
	reqClone.Header.Set("Connection", "close") // Matches Python line 75: session.headers.update({"Connection": "close"})

	// Get token and add if available (matches Python line 56-60)
	token, err := t.tokenProvider.GetToken(ctx)
	if err == nil && token != "" {
		// Token acquired successfully - add Authorization header (matches Python line 58)
		reqClone.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	} else {
		// Failed to get token - log warning but continue (matches Python line 60)
		log.Printf("Failed to retrieve authentication token! Request might fail.")
		// Python continues without Authorization header, so we do too
	}

	// Execute request (retries handled by go-retryablehttp wrapper)
	return t.base.RoundTrip(reqClone)
}
