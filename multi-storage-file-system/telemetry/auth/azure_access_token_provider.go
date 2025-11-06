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

package auth

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/AzureAD/microsoft-authentication-library-for-go/apps/confidential"
)

const (
	// Retry configuration - matches Python MSC constants
	maxRetries      = 5     // MAX_RETRIES = 5
	backoffFactorMs = 500   // BACKOFF_FACTOR = 0.5 seconds = 500ms
	maxBackoffMs    = 60000 // Maximum backoff time = 60 seconds = 60000ms
)

// `AzureAccessTokenProvider` provides Azure AD access tokens using MSAL.
// Matches Python: `multistorageclient.instrumentation.auth.AzureAccessTokenProvider`
//
// Caching behavior:
//
//	Python: acquire_token_for_client() automatically checks cache (since msal v1.23+)
//	Go: Must explicitly call AcquireTokenSilent() first, then AcquireTokenByCredential()
//
// The `GetToken()` method handles this difference to provide equivalent behavior.
type AzureAccessTokenProvider struct {
	client confidential.Client
	scopes []string
}

// `Config` contains Azure AD authentication configuration.
type Config struct {
	ClientID         string
	ClientCredential string
	Authority        string
	Scopes           []string
}

// `NewAzureAccessTokenProvider` creates a new Azure token provider.
// Matches Python's AzureAccessTokenProvider with retry configuration for MSAL HTTP client.
func NewAzureAccessTokenProvider(config Config) (*AzureAccessTokenProvider, error) {
	cred, err := confidential.NewCredFromSecret(config.ClientCredential)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}

	// Create custom HTTP client with retry logic for MSAL token acquisition
	// Matches Python lines 64-72 in instrumentation/auth.py
	httpClient := &http.Client{
		Transport: &msalRetryTransport{
			base:         http.DefaultTransport,
			retryMax:     maxRetries,
			retryWaitMin: time.Duration(backoffFactorMs) * time.Millisecond,
			retryWaitMax: time.Duration(maxBackoffMs) * time.Millisecond,
		},
	}

	// Create MSAL client with custom HTTP client (matches Python passing http_client to msal)
	client, err := confidential.New(
		config.Authority,
		config.ClientID,
		cred,
		confidential.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create MSAL client: %w", err)
	}

	return &AzureAccessTokenProvider{
		client: client,
		scopes: config.Scopes,
	}, nil
}

// msalRetryTransport implements http.RoundTripper with retry logic for MSAL requests.
// Matches Python's requests.Session with Retry(status_forcelist=[408, 429, 500, 501, 502, 503, 504])
type msalRetryTransport struct {
	base         http.RoundTripper
	retryMax     int
	retryWaitMin time.Duration
	retryWaitMax time.Duration
}

func (t *msalRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= t.retryMax; attempt++ {
		resp, err := t.base.RoundTrip(req)

		// Success case
		if err == nil {
			// Check status code - retry on status_forcelist
			switch resp.StatusCode {
			case 408, 429, 500, 501, 502, 503, 504:
				resp.Body.Close()
				lastErr = fmt.Errorf("server returned status %d", resp.StatusCode)
			default:
				return resp, nil // Success
			}
		} else {
			lastErr = err
		}

		// Don't sleep after last attempt
		if attempt < t.retryMax {
			backoff := t.calculateBackoff(attempt)
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("MSAL request failed after %d attempts: %w", t.retryMax+1, lastErr)
}

func (t *msalRetryTransport) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: retryWaitMin * 2^attempt, capped at retryWaitMax
	multiplier := 1 << uint(attempt)
	backoff := time.Duration(float64(t.retryWaitMin) * float64(multiplier))
	if backoff > t.retryWaitMax {
		backoff = t.retryWaitMax
	}
	return backoff
}

// `GetToken` returns a valid access token.
// Matches Python: `get_token() -> str`
//
// Go MSAL caching pattern (different from Python):
//  1. Try `AcquireTokenSilent()` first (checks cache)
//  2. On cache miss, call `AcquireTokenByCredential()` (acquires and caches new token)
//
// Python's `msal.ConfidentialClientApplication.acquire_token_for_client()` does both
// automatically (checks cache, acquires if needed). Go requires explicit cache check.
//
// Implements retry logic with exponential backoff matching Python:
// - Retries up to maxRetries (5) times
// - Exponential backoff: backoffFactor * (2 ^ retryCount)
// - Maximum backoff capped at maxBackoffSecs (60s)
func (p *AzureAccessTokenProvider) GetToken(ctx context.Context) (string, error) {
	// Try cache first (mimics Python's automatic cache check)
	// AcquireTokenSilent returns cached token if valid, error if cache miss
	result, err := p.client.AcquireTokenSilent(ctx, p.scopes)
	if err == nil && result.AccessToken != "" {
		// Cache hit - return immediately
		return result.AccessToken, nil
	}

	// Cache miss - acquire new token with retry logic (matches Python)
	retryCount := 0
	for retryCount < maxRetries {
		// Acquire new token - this will cache it for future AcquireTokenSilent() calls
		result, err := p.client.AcquireTokenByCredential(ctx, p.scopes)

		if err == nil {
			// Success path - check for access token (matches Python line 80-88)
			if result.AccessToken != "" {
				return result.AccessToken, nil
			}
			// No error but no token - return immediately (matches Python line 83-88)
			return "", fmt.Errorf("MSAL returned empty access token")
		}

		// Error occurred (matches Python line 89-99)
		// Python behavior: ONLY retry on ConnectionError (line 89-96)
		// All other errors return immediately (line 97-99)

		// Check if this is a connection error (matches Python ConnectionError)
		if !isConnectionError(err) {
			// Not a connection error - return immediately (matches Python line 97-99)
			return "", fmt.Errorf("failed to acquire token: %w", err)
		}

		// Connection error - retry with backoff (matches Python line 91-96)
		retryCount++

		// Only sleep if we're going to retry (matches Python line 94-96)
		if retryCount < maxRetries {
			// Calculate backoff: min(backoffFactorMs * 2^retryCount, maxBackoffMs)
			// Python uses 2^retryCount AFTER increment (line 95)
			// backoffFactorMs is 500ms, so: 500ms, 1s, 2s, 4s, 8s...
			backoffMs := float64(backoffFactorMs) * math.Pow(2, float64(retryCount))
			if backoffMs > float64(maxBackoffMs) {
				backoffMs = float64(maxBackoffMs)
			}
			sleepDuration := time.Duration(backoffMs) * time.Millisecond

			time.Sleep(sleepDuration)
		}
	}

	// All retries exhausted (matches Python line 101-102)
	return "", fmt.Errorf("failed to acquire token after %d retries", maxRetries)
}

// isConnectionError checks if an error is a network/connection error.
// Matches Python's `requests.exceptions.ConnectionError` behavior (line 89).
//
// Python's ConnectionError includes:
// - DNS resolution failures
// - Connection refused
// - Connection timeouts
// - Network unreachable
// - Other socket-level errors
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for net.Error (includes DNS, dial, timeout errors)
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for syscall errors (ECONNREFUSED, ENETUNREACH, etc.)
	var syscallErr syscall.Errno
	if errors.As(err, &syscallErr) {
		return true
	}

	// Check error message for common connection-related strings
	errMsg := strings.ToLower(err.Error())
	connectionKeywords := []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"no such host",
		"network is unreachable",
		"dial tcp",
		"i/o timeout",
		"broken pipe",
	}

	for _, keyword := range connectionKeywords {
		if strings.Contains(errMsg, keyword) {
			return true
		}
	}

	return false
}
