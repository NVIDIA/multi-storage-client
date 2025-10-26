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
	"fmt"
	"log"
	"math"
	"time"

	"github.com/AzureAD/microsoft-authentication-library-for-go/apps/confidential"
)

const (
	// Retry configuration - matches Python MSC constants
	maxRetries     = 5   // MAX_RETRIES = 5
	backoffFactor  = 0.5 // BACKOFF_FACTOR = 0.5
	maxBackoffSecs = 60  // Maximum backoff time in seconds
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
func NewAzureAccessTokenProvider(config Config) (*AzureAccessTokenProvider, error) {
	cred, err := confidential.NewCredFromSecret(config.ClientCredential)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %w", err)
	}

	client, err := confidential.New(config.Authority, config.ClientID, cred)
	if err != nil {
		return nil, fmt.Errorf("failed to create MSAL client: %w", err)
	}

	return &AzureAccessTokenProvider{
		client: client,
		scopes: config.Scopes,
	}, nil
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
			// No error but no token - log warning and return immediately (matches Python line 83-88)
			log.Printf("MSAL token acquisition returned no error but empty access token")
			return "", fmt.Errorf("MSAL returned empty access token")
		}

		// Error occurred (matches Python line 89-99)
		// Python behavior: ONLY retry on ConnectionError (line 89-96)
		// All other errors return immediately (line 97-99)
		//
		// In Go, we retry all errors since Go's MSAL library error types may differ
		// from Python's requests.exceptions.ConnectionError

		log.Printf("Getting token attempt %d failed with error: %v", retryCount+1, err)
		retryCount++

		// Only sleep if we're going to retry (matches Python line 94-96)
		if retryCount < maxRetries {
			// Calculate backoff: min(backoffFactor * 2^retryCount, maxBackoffSecs)
			// Python uses 2^retryCount AFTER increment (line 95)
			sleepTime := backoffFactor * math.Pow(2, float64(retryCount))
			if sleepTime > maxBackoffSecs {
				sleepTime = maxBackoffSecs
			}
			sleepDuration := time.Duration(sleepTime * float64(time.Second))

			log.Printf("Retrying after %v...", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}

	// All retries exhausted (matches Python line 101-102)
	log.Printf("All %d token fetch attempts failed", maxRetries)
	return "", fmt.Errorf("failed to acquire token after %d retries", maxRetries)
}
