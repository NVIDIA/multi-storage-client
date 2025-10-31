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
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NVIDIA/multi-storage-client/posix/fuse/mscp/telemetry/auth"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	// HTTP retry configuration - matches Python _OTLPMSALMetricExporter
	maxRetries          = 5    // _MAX_RETRIES = 5
	retryWaitMin        = 500  // milliseconds (backoff_factor = 0.5 → first retry 0.5s)
	retryWaitMax        = 8000 // milliseconds (max backoff: 0.5 * 2^4 = 8s)
	tokenAcquireTimeout = 30   // seconds (timeout for acquiring access token)
)

// `NewOTLPMSALExporter` creates an OTLP exporter with MSAL auth and automatic retries.
// Matches Python: `_OTLPMSALMetricExporter.__init__(auth, exporter)`
func NewOTLPMSALExporter(authConfig auth.Config, endpoint string) (sdkmetric.Exporter, error) {
	tokenProvider, err := auth.NewAzureAccessTokenProvider(authConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create token provider: %w", err)
	}

	httpClient := &http.Client{
		Transport: &retryableTransport{
			tokenProvider: tokenProvider,
			retryMax:      maxRetries,
			retryWaitMin:  retryWaitMin * time.Millisecond,
			retryWaitMax:  retryWaitMax * time.Millisecond,
		},
	}

	// Parse endpoint to extract host and path
	// Supports: "https://hostname/v1/metrics" or "hostname:port"
	host, path, isInsecure := parseEndpoint(endpoint)

	opts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(host),
		otlpmetrichttp.WithHTTPClient(httpClient),
	}

	if path != "" {
		opts = append(opts, otlpmetrichttp.WithURLPath(path))
	}

	// If endpoint uses http:// (not https://), use insecure mode
	if isInsecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}

	return otlpmetrichttp.New(context.Background(), opts...)
}

// parseEndpoint extracts host, path, and scheme from endpoint string.
// Examples:
//
//	"https://host.com/v1/metrics" → ("host.com", "/v1/metrics", false)
//	"http://host.com:4318/v1/metrics" → ("host.com:4318", "/v1/metrics", true)
//	"host.com:4318" → ("host.com:4318", "", false) [defaults to https]
func parseEndpoint(endpoint string) (host, path string, isInsecure bool) {
	// If endpoint doesn't have a scheme, prepend https:// for parsing
	originalEndpoint := endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		// Parse failed, return original endpoint as host, assume secure
		return originalEndpoint, "", false
	}

	// Determine if insecure (http://) or secure (https://)
	isInsecure = (u.Scheme == "http")

	return u.Host, u.Path, isInsecure
}

// retryableTransport implements http.RoundTripper with retry logic and MSAL auth.
// Matches Python's AccessTokenHTTPAdapter with requests.adapters.Retry.
type retryableTransport struct {
	tokenProvider *auth.AzureAccessTokenProvider
	retryMax      int
	retryWaitMin  time.Duration
	retryWaitMax  time.Duration
}

// RoundTrip implements http.RoundTripper with retry logic.
// Matches Python's HTTPAdapter.send() with automatic retries.
func (t *retryableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token := t.getToken()

	var lastErr error
	for attempt := 0; attempt <= t.retryMax; attempt++ {
		reqClone := t.prepareRequest(req, token)

		resp, err := http.DefaultTransport.RoundTrip(reqClone)

		if err == nil && resp.StatusCode < 500 {
			return resp, nil
		}

		lastErr = t.handleError(err, resp, attempt)

		if attempt < t.retryMax {
			t.sleepWithBackoff(attempt)
		}
	}

	return nil, fmt.Errorf("giving up after %d attempt(s): %w", t.retryMax+1, lastErr)
}

func (t *retryableTransport) getToken() string {
	ctx, cancel := context.WithTimeout(context.Background(), tokenAcquireTimeout*time.Second)
	defer cancel()

	token, err := t.tokenProvider.GetToken(ctx)
	if err != nil || token == "" {
		return ""
	}

	return token
}

func (t *retryableTransport) prepareRequest(req *http.Request, token string) *http.Request {
	// Preserve request context but remove any deadline to prevent premature timeouts
	// This maintains cancellation, tracing, and context values while allowing retries
	ctx := req.Context()
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		// Use context.WithoutCancel (Go 1.21+) to preserve context values without deadline
		ctx = context.WithoutCancel(ctx)
	}

	reqClone := req.Clone(ctx)
	reqClone.Header.Set("Connection", "close")
	if token != "" {
		reqClone.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}
	return reqClone
}

func (t *retryableTransport) handleError(err error, resp *http.Response, attempt int) error {
	var lastErr error
	if err != nil {
		lastErr = fmt.Errorf("attempt %d: %w", attempt+1, err)
	} else {
		// Read response body for detailed error message
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		bodyPreview := ""
		if readErr == nil && len(bodyBytes) > 0 {
			bodyPreview = string(bodyBytes)
			if len(bodyPreview) > 500 {
				bodyPreview = bodyPreview[:500] + "..."
			}
		}

		lastErr = fmt.Errorf("attempt %d: server returned status %d", attempt+1, resp.StatusCode)

		// Include body preview in error for debugging
		if bodyPreview != "" {
			lastErr = fmt.Errorf("%w: %s", lastErr, bodyPreview)
		}
	}
	return lastErr
}

func (t *retryableTransport) sleepWithBackoff(attempt int) {
	multiplier := 1 << uint(attempt)
	backoff := time.Duration(float64(t.retryWaitMin) * float64(multiplier))
	if backoff > t.retryWaitMax {
		backoff = t.retryWaitMax
	}
	time.Sleep(backoff)
}
