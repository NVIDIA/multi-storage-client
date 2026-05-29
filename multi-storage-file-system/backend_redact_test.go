package main

import (
	"strings"
	"testing"
)

// newRedactBackend builds a minimal backendStruct wired to the given context for
// exercising redactSecrets without constructing any real cloud client.
func newRedactBackend(specifics interface{}, makeContext func(b *backendStruct) backendContextIf) *backendStruct {
	b := &backendStruct{backendTypeSpecifics: specifics}
	b.context = makeContext(b)
	return b
}

func TestRedactSecretsNilBackendAppliesAWSHeuristic(t *testing.T) {
	// AWS-shaped access key ID and 40-char secret should be redacted even when
	// no backend context is available (config-parse error path).
	in := "auth failed for AKIAIOSFODNN7EXAMPLE using wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY12" //nolint:gosec // G101: fake credentials for redaction test
	out := redactSecrets(nil, in)

	if strings.Contains(out, "AKIAIOSFODNN7EXAMPLE") {
		t.Errorf("access key ID not redacted: %q", out)
	}
	if !strings.Contains(out, "***REDACTED-AWS-ACCESS-KEY-ID***") {
		t.Errorf("expected access-key placeholder, got: %q", out)
	}
	if !strings.Contains(out, "***REDACTED-AWS-SECRET-ACCESS-KEY***") {
		t.Errorf("expected secret-key placeholder, got: %q", out)
	}
}

func TestRedactSecretsS3ValueBased(t *testing.T) {
	// A configured secret that is NOT AWS-shaped must still be redacted by the
	// S3 backend via exact value matching.
	const secret = "not-an-aws-shaped-secret-but-still-sensitive" //nolint:gosec // G101: fake test value
	const accessKeyID = "AKIAIOSFODNN7EXAMPLE"                    //nolint:gosec // G101: fake test value
	b := newRedactBackend(
		&backendConfigS3Struct{accessKeyID: accessKeyID, secretAccessKey: secret},
		func(b *backendStruct) backendContextIf { return &s3ContextStruct{backend: b} },
	)

	out := redactSecrets(b, "listing failed with creds "+secret+" / "+accessKeyID)
	if strings.Contains(out, secret) {
		t.Errorf("configured secret value not redacted: %q", out)
	}
	if strings.Contains(out, accessKeyID) {
		t.Errorf("access key ID not redacted: %q", out)
	}
}

func TestRedactSecretsGCSAndAIStore(t *testing.T) {
	const gcsKey = "AIzaSyD-ExampleGcsApiKeyValue1234567890x" //nolint:gosec // G101: fake test value
	gcs := newRedactBackend(
		&backendConfigGCSStruct{apiKey: gcsKey},
		func(b *backendStruct) backendContextIf { return &gcsContextStruct{backend: b} },
	)
	if out := redactSecrets(gcs, "gcs error: "+gcsKey); strings.Contains(out, gcsKey) {
		t.Errorf("GCS api key not redacted: %q", out)
	}

	const aisToken = "eyJhbGciOiExampleAisAuthnTokenValue.payload.sig" //nolint:gosec // G101: fake test value
	ais := newRedactBackend(
		&backendConfigAIStoreStruct{authnToken: aisToken},
		func(b *backendStruct) backendContextIf { return &aistoreContextStruct{backend: b} },
	)
	if out := redactSecrets(ais, "ais error: "+aisToken); strings.Contains(out, aisToken) {
		t.Errorf("AIStore authn token not redacted: %q", out)
	}
}

func TestRedactSecretsPseudoAndRAMPassThrough(t *testing.T) {
	pseudo := &backendStruct{context: &pseudoContextStruct{}}
	ram := &backendStruct{context: &ramContextStruct{}}

	const msg = "no secrets here, just a message"
	if got := redactSecrets(pseudo, msg); got != msg {
		t.Errorf("pseudo backend should pass through, got: %q", got)
	}
	if got := redactSecrets(ram, msg); got != msg {
		t.Errorf("ram backend should pass through, got: %q", got)
	}
}

func TestRedactValueIgnoresShortSecrets(t *testing.T) {
	// Secrets shorter than 8 chars are ignored to avoid redacting incidental
	// substrings (and empty values, which would otherwise match everywhere).
	if got := redactValue("hello world", "short", "X"); got != "hello world" {
		t.Errorf("short secret should be ignored, got: %q", got)
	}
	if got := redactValue("hello world", "", "X"); got != "hello world" {
		t.Errorf("empty secret should be ignored, got: %q", got)
	}
	if got := redactValue("a longsecretvalue b", "longsecretvalue", "X"); got != "a X b" {
		t.Errorf("expected redaction, got: %q", got)
	}
}
