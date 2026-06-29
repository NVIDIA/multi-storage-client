package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// writeAISTokenFile writes token to path in the same plain-JSON format the AIS
// authn token file uses, so authn.LoadToken can read it back.
func writeAISTokenFile(t *testing.T, path, token string) {
	t.Helper()
	if err := jsp.SaveMeta(path, &authn.TokenMsg{Token: token}, nil); err != nil {
		t.Fatalf("failed to write AIS token file %q: %v", path, err)
	}
}

// An inline (authn_token) or absent (anonymous) token has no authnTokenFile, so
// currentBaseParams must return the setup parameters unchanged with no file I/O.
func TestCurrentBaseParams_NoTokenFileReturnsUnchanged(t *testing.T) {
	aisContext := &aistoreContextStruct{
		baseParams: api.BaseParams{Token: "inline-or-anonymous"}, //nolint:gosec // G101: fake test value
	}
	if got := aisContext.currentBaseParams().Token; got != "inline-or-anonymous" {
		t.Fatalf("token = %q, want the fixed setup token (no file reload)", got)
	}
}

// A file-based token must be re-read when the token file's mtime changes (JWT
// rotation), and served from cache when it has not changed.
func TestCurrentBaseParams_ReloadsOnTokenFileRotation(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "auth.token")
	writeAISTokenFile(t, tokenPath, "token-1")

	info, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}

	// Simulate setup: token already loaded, baseline mtime recorded.
	aisContext := &aistoreContextStruct{
		baseParams:     api.BaseParams{Token: "token-1"},
		authnTokenFile: tokenPath,
		tokenMTime:     info.ModTime(),
	}

	// Unchanged file -> cached token, no reload.
	if got := aisContext.currentBaseParams().Token; got != "token-1" {
		t.Fatalf("unchanged file: token = %q, want token-1", got)
	}

	// Rotate the token, forcing a strictly newer mtime so the change is detected
	// deterministically regardless of filesystem timestamp resolution.
	writeAISTokenFile(t, tokenPath, "token-2")
	future := info.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(tokenPath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if got := aisContext.currentBaseParams().Token; got != "token-2" {
		t.Fatalf("after rotation: token = %q, want token-2 (reloaded)", got)
	}

	// With no further change the refreshed token is retained.
	if got := aisContext.currentBaseParams().Token; got != "token-2" {
		t.Fatalf("steady state: token = %q, want token-2", got)
	}
}
