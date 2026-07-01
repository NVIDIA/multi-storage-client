package driver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- credential-mode resolution ----------------------------------------------

func TestResolveCredentialMode_AutoStaticWhenSecretPresent(t *testing.T) {
	mode, err := resolveCredentialMode(map[string]string{}, map[string]string{
		"access_key_id":     "AKIA...",
		"secret_access_key": "secret",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != credentialModeStatic {
		t.Fatalf("mode = %q, want %q", mode, credentialModeStatic)
	}
}

func TestResolveCredentialMode_AutoWorkloadIdentityWhenNoSecret(t *testing.T) {
	mode, err := resolveCredentialMode(map[string]string{}, map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != credentialModeIRSA {
		t.Fatalf("mode = %q, want %q", mode, credentialModeIRSA)
	}
}

func TestResolveCredentialMode_StaticRequiresBothKeys(t *testing.T) {
	cases := []struct {
		name    string
		secrets map[string]string
	}{
		{"missing_secret_access_key", map[string]string{"access_key_id": "AKIA..."}},
		{"missing_access_key_id", map[string]string{"secret_access_key": "secret"}},
		{"empty_values", map[string]string{"access_key_id": "", "secret_access_key": ""}},
		{"no_secret_at_all", map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveCredentialMode(map[string]string{"authType": "static"}, tc.secrets)
			if err == nil {
				t.Fatalf("expected error for incomplete static credentials, got nil")
			}
			if !strings.Contains(err.Error(), "access_key_id") || !strings.Contains(err.Error(), "secret_access_key") {
				t.Fatalf("error message should reference both required keys, got: %v", err)
			}
		})
	}
}

func TestResolveCredentialMode_IrsaDoesNotRequireSecret(t *testing.T) {
	for _, alias := range []string{"irsa", "IRSA", "wif", "WIF", " irsa "} {
		t.Run(alias, func(t *testing.T) {
			mode, err := resolveCredentialMode(map[string]string{"authType": alias}, map[string]string{})
			if err != nil {
				t.Fatalf("unexpected error for authType=%q: %v", alias, err)
			}
			if mode != credentialModeIRSA {
				t.Fatalf("mode = %q, want %q", mode, credentialModeIRSA)
			}
		})
	}
}

func TestResolveCredentialMode_NoneAndAnonymous(t *testing.T) {
	for _, alias := range []string{"none", "NONE", "anonymous", " none "} {
		t.Run(alias, func(t *testing.T) {
			mode, err := resolveCredentialMode(map[string]string{"authType": alias}, map[string]string{})
			if err != nil {
				t.Fatalf("unexpected error for authType=%q: %v", alias, err)
			}
			if mode != credentialModeNone {
				t.Fatalf("mode = %q, want %q", mode, credentialModeNone)
			}
		})
	}
}

func TestResolveCredentialMode_RejectsUnknownAuthType(t *testing.T) {
	_, err := resolveCredentialMode(map[string]string{"authType": "magic"}, map[string]string{})
	if err == nil {
		t.Fatalf("expected error for unknown authType, got nil")
	}
	if !strings.Contains(err.Error(), "magic") {
		t.Fatalf("error message should mention the offending value, got: %v", err)
	}
}

// --- writeConfig content per mode --------------------------------------------

func TestWriteConfig_StaticModeWritesStaticCredentialPlaceholders(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{"bucketName": "my-bucket"},
		map[string]string{"access_key_id": "AKIA...", "secret_access_key": "secret"},
		credentialModeStatic,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	if !strings.Contains(body, `access_key_id: "${AWS_ACCESS_KEY_ID}"`) {
		t.Fatalf("static mode config should include access_key_id placeholder; got:\n%s", body)
	}
	if !strings.Contains(body, `secret_access_key: "${AWS_SECRET_ACCESS_KEY}"`) {
		t.Fatalf("static mode config should include secret_access_key placeholder; got:\n%s", body)
	}
}

func TestWriteConfig_WorkloadIdentityModeOmitsStaticCredentialPlaceholders(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{"bucketName": "my-bucket"},
		map[string]string{},
		credentialModeIRSA,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	if strings.Contains(body, "access_key_id") {
		t.Fatalf("IRSA mode config must NOT contain access_key_id; got:\n%s", body)
	}
	if strings.Contains(body, "secret_access_key") {
		t.Fatalf("IRSA mode config must NOT contain secret_access_key; got:\n%s", body)
	}
	if !strings.Contains(body, "bucket_container_name: my-bucket") {
		t.Fatalf("config should still describe the bucket; got:\n%s", body)
	}
}

func TestWriteConfig_AIStoreBackend(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{
			"backendType":           "AIStore",
			"bucketName":            "ais-bucket",
			"prefix":                "datasets/",
			"aisEndpoint":           "https://ais.example.com",
			"aisProvider":           "ais",
			"aisAuthnTokenFile":     "/var/run/secrets/ais/token",
			"aisManifestGenBackend": "direct-s3",
		},
		map[string]string{},
		credentialModeIRSA,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	for _, want := range []string{
		"dir_name: ais",
		"bucket_container_name: ais-bucket",
		`prefix: "datasets/"`,
		"backend_type: AIStore",
		"AIStore:",
		`endpoint: "https://ais.example.com"`,
		`provider: "ais"`,
		`authn_token_file: "/var/run/secrets/ais/token"`,
		`manifest_gen_backend: "direct-s3"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("AIStore config missing %q; got:\n%s", want, body)
		}
	}
	if strings.Contains(body, "S3:") || strings.Contains(body, "access_key_id") || strings.Contains(body, "secret_access_key") {
		t.Fatalf("AIStore config should not include S3/static credential config; got:\n%s", body)
	}
}

func TestWriteConfig_NoneModeS3EmitsAnonymous(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{"bucketName": "public-bucket"},
		map[string]string{},
		credentialModeNone,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	if !strings.Contains(body, "anonymous: true") {
		t.Fatalf("none mode S3 config should set anonymous: true; got:\n%s", body)
	}
	if strings.Contains(body, "access_key_id") || strings.Contains(body, "secret_access_key") {
		t.Fatalf("none mode config must NOT contain static credential placeholders; got:\n%s", body)
	}
}

func TestWriteConfig_NoneModeAIStoreOmitsAnonymousAndCreds(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{
			"backendType": "AIStore",
			"bucketName":  "ais-bucket",
			"aisEndpoint": "https://ais.example.com",
		},
		map[string]string{},
		credentialModeNone,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	if !strings.Contains(body, "backend_type: AIStore") {
		t.Fatalf("expected AIStore backend; got:\n%s", body)
	}
	// anonymous is an S3-only field; for AIStore, no-credentials means an empty
	// token, so neither an anonymous flag nor a token should be emitted.
	if strings.Contains(body, "anonymous:") || strings.Contains(body, "authn_token") {
		t.Fatalf("none mode AIStore config must not emit anonymous/token; got:\n%s", body)
	}
}

func TestWriteConfig_NoneModeAIStoreIgnoresTokenInputs(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	// Even when AIStore token attributes are supplied, none (anonymous) mode
	// must not emit them — the backend connects with an empty token.
	dir, configPath := writeConfigOrFatal(t, ns,
		map[string]string{
			"backendType":       "AIStore",
			"bucketName":        "ais-bucket",
			"aisEndpoint":       "https://ais.example.com",
			"aisAuthnToken":     "inline-token",
			"aisAuthnTokenFile": "/var/run/secrets/ais/token",
		},
		map[string]string{},
		credentialModeNone,
	)
	defer os.RemoveAll(dir)

	body := readFileOrFatal(t, configPath)
	if strings.Contains(body, "authn_token") {
		t.Fatalf("none mode must ignore AIStore token inputs; got:\n%s", body)
	}
}

func TestWriteConfig_RejectsUnsupportedBackendType(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	dir, _, err := ns.writeConfig("/tmp/csi-target-test",
		map[string]string{"backendType": "GCS", "bucketName": "bucket"},
		map[string]string{},
		false,
		credentialModeIRSA,
	)
	if err == nil {
		defer os.RemoveAll(dir)
		t.Fatalf("writeConfig unexpectedly accepted unsupported backendType")
	}
	if !strings.Contains(err.Error(), "unsupported backendType") {
		t.Fatalf("error should mention unsupported backendType, got: %v", err)
	}
}

// --- buildEnv content per mode -----------------------------------------------

func TestBuildEnv_StaticIncludesAwsEnvVars(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	env := ns.buildEnv(map[string]string{
		"access_key_id":     "AKIA...",
		"secret_access_key": "secret",
		"session_token":     "session-token",
	}, credentialModeStatic, "", "")
	if !envHasPrefix(env, "AWS_ACCESS_KEY_ID=AKIA...") {
		t.Errorf("expected AWS_ACCESS_KEY_ID in env; got %v", env)
	}
	if !envHasPrefix(env, "AWS_SECRET_ACCESS_KEY=secret") {
		t.Errorf("expected AWS_SECRET_ACCESS_KEY in env; got %v", env)
	}
	if !envHasPrefix(env, "AWS_SESSION_TOKEN=session-token") {
		t.Errorf("expected AWS_SESSION_TOKEN in env; got %v", env)
	}
}

func TestBuildEnv_WorkloadIdentityOmitsStaticAwsEnvVars(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	// Even when a secret is provided, IRSA mode must not propagate it. This
	// guards against authType=irsa being set on a PV that also references a
	// stale secret: we want the SDK credential chain to win in that case.
	// Assert against the runner's own environment (which may legitimately export
	// AWS_* vars) rather than absolute absence, so the test is deterministic.
	base := os.Environ()
	env := ns.buildEnv(map[string]string{
		"access_key_id":     "AKIA-stale",
		"secret_access_key": "stale-secret",
		"session_token":     "stale-session",
	}, credentialModeIRSA, "", "")
	for _, key := range []string{"AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN="} {
		if got, want := envCount(env, key), envCount(base, key); got != want {
			t.Errorf("IRSA mode must not add %s entries; before=%d after=%d", key, want, got)
		}
	}
	for _, stale := range []string{
		"AWS_ACCESS_KEY_ID=AKIA-stale",
		"AWS_SECRET_ACCESS_KEY=stale-secret",
		"AWS_SESSION_TOKEN=stale-session",
	} {
		if envHasPrefix(env, stale) {
			t.Errorf("IRSA mode must NOT inject stale secret value %s; got env %v", stale, env)
		}
	}
}

func TestBuildEnv_NoneOmitsAwsEnvVars(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	base := os.Environ()
	// none mode must not inject any AWS credential env vars, even if a secret
	// was somehow supplied.
	env := ns.buildEnv(map[string]string{
		"access_key_id":     "AKIA-ignored",
		"secret_access_key": "ignored-secret",
	}, credentialModeNone, "", "")
	for _, key := range []string{"AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN="} {
		if got, want := envCount(env, key), envCount(base, key); got != want {
			t.Errorf("none mode must not add %s entries; before=%d after=%d", key, want, got)
		}
	}
}

// --- per-workload IRSA (NGCDP-8824) ------------------------------------------

func TestParseWorkloadToken_AbsentReturnsNotOkNoError(t *testing.T) {
	_, ok, err := parseWorkloadToken(map[string]string{}, stsAudience)
	if err != nil {
		t.Fatalf("unexpected error when tokens key absent: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false when tokens key absent (fallback signal)")
	}
}

func TestParseWorkloadToken_RejectsMissingAudience(t *testing.T) {
	volCtx := map[string]string{
		serviceAccountTokensVolCtxKey: `{"some.other.audience":{"token":"abc","expirationTimestamp":"2026-06-01T11:00:00Z"}}`,
	}
	_, _, err := parseWorkloadToken(volCtx, stsAudience)
	if err == nil {
		t.Fatalf("expected error when requested audience is missing, got nil")
	}
	if !strings.Contains(err.Error(), stsAudience) {
		t.Fatalf("error should mention the missing audience %q, got: %v", stsAudience, err)
	}
}

func TestParseWorkloadToken_RejectsMalformedJSON(t *testing.T) {
	volCtx := map[string]string{serviceAccountTokensVolCtxKey: `not-json`}
	if _, _, err := parseWorkloadToken(volCtx, stsAudience); err == nil {
		t.Fatalf("expected error for malformed tokens JSON, got nil")
	}
}

func TestResolveWorkloadIdentity_WritesTokenFile(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	volCtx := map[string]string{
		"roleArn":                     "arn:aws:iam::123456789012:role/team-a",
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"workload-token-xyz","expirationTimestamp":"2026-06-01T11:00:00Z"}}`,
	}
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeIRSA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := filepath.Join(configDir, webIdentityTokenFileName)
	if tokenFile != want {
		t.Fatalf("tokenFile = %q, want %q", tokenFile, want)
	}
	if roleArn != "arn:aws:iam::123456789012:role/team-a" {
		t.Fatalf("roleArn = %q, want the volumeAttributes value", roleArn)
	}
	if body := readFileOrFatal(t, tokenFile); body != "workload-token-xyz" {
		t.Fatalf("token file content = %q, want the workload token", body)
	}
	info, statErr := os.Stat(tokenFile)
	if statErr != nil {
		t.Fatalf("stat token file: %v", statErr)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("token file mode = %o, want 0600", perm)
	}
}

func TestResolveWorkloadIdentity_OverridesEnv(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	volCtx := map[string]string{
		"roleArn":                     "arn:aws:iam::123456789012:role/team-a",
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"workload-token-xyz"}}`,
	}
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeIRSA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate the driver pod's own IRSA env being inherited; the per-workload
	// values must replace (not duplicate) it so the child assumes the
	// workload's role, not the driver SA's.
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/eks.amazonaws.com/serviceaccount/token")
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/msfs-csi-node")

	env := ns.buildEnv(map[string]string{}, credentialModeIRSA, tokenFile, roleArn)

	if got := envCount(env, "AWS_WEB_IDENTITY_TOKEN_FILE="); got != 1 {
		t.Fatalf("AWS_WEB_IDENTITY_TOKEN_FILE appears %d times, want exactly 1 (override)", got)
	}
	if v := envValue(env, "AWS_WEB_IDENTITY_TOKEN_FILE="); v != tokenFile {
		t.Fatalf("AWS_WEB_IDENTITY_TOKEN_FILE = %q, want %q", v, tokenFile)
	}
	if got := envCount(env, "AWS_ROLE_ARN="); got != 1 {
		t.Fatalf("AWS_ROLE_ARN appears %d times, want exactly 1 (override)", got)
	}
	if v := envValue(env, "AWS_ROLE_ARN="); v != roleArn {
		t.Fatalf("AWS_ROLE_ARN = %q, want %q", v, roleArn)
	}
}

func TestResolveWorkloadIdentity_RepublishRewritesFile(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	mk := func(token string) map[string]string {
		return map[string]string{
			"roleArn":                     "arn:aws:iam::123456789012:role/team-a",
			serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"` + token + `"}}`,
		}
	}
	first, _, err := ns.resolveWorkloadIdentity(configDir, mk("token-1"), credentialModeIRSA)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	// A republish call delivers a refreshed token; the driver rewrites the same
	// file in place (no new path, no re-spawn).
	second, _, err := ns.resolveWorkloadIdentity(configDir, mk("token-2"), credentialModeIRSA)
	if err != nil {
		t.Fatalf("second (republish) call: %v", err)
	}
	if first != second {
		t.Fatalf("republish wrote a new path %q (was %q); must rewrite in place", second, first)
	}
	if body := readFileOrFatal(t, second); body != "token-2" {
		t.Fatalf("token file content = %q, want the refreshed token-2", body)
	}
}

func TestResolveWorkloadIdentity_FallbackWhenTokensKeyMissing(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	// IRSA mode but no tokens key (older kubelet / per-workload IRSA disabled).
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir,
		map[string]string{"roleArn": "arn:aws:iam::123456789012:role/team-a"}, credentialModeIRSA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokenFile != "" || roleArn != "" {
		t.Fatalf("expected no-op fallback (\"\",\"\"); got tokenFile=%q roleArn=%q", tokenFile, roleArn)
	}
	if _, statErr := os.Stat(filepath.Join(configDir, webIdentityTokenFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("no token file should be written in fallback; stat err = %v", statErr)
	}
}

func TestResolveWorkloadIdentity_RequiresRoleArn(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	volCtx := map[string]string{
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"workload-token-xyz"}}`,
	}
	_, _, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeIRSA)
	if err == nil {
		t.Fatalf("expected error when roleArn missing for per-workload IRSA, got nil")
	}
	if !strings.Contains(err.Error(), "roleArn") {
		t.Fatalf("error should mention roleArn, got: %v", err)
	}
}

func TestResolveWorkloadIdentity_StaticModeIsNoop(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	volCtx := map[string]string{
		"roleArn":                     "arn:aws:iam::123456789012:role/team-a",
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"ignored"}}`,
	}
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeStatic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokenFile != "" || roleArn != "" {
		t.Fatalf("static mode must be a no-op; got tokenFile=%q roleArn=%q", tokenFile, roleArn)
	}
}

func TestResolveWorkloadIdentity_AIStoreBackendBypassesWorkloadIdentity(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	// On a tokenRequests-enabled CSIDriver the kubelet delivers a workload token
	// for every mount, AIStore included. AIStore needs no AWS identity, so this
	// must be a no-op and must NOT require volumeAttributes.roleArn.
	volCtx := map[string]string{
		"backendType":                 "AIStore",
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"workload-token-xyz"}}`,
	}
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeIRSA)
	if err != nil {
		t.Fatalf("AIStore must not require per-workload IRSA / roleArn; got error: %v", err)
	}
	if tokenFile != "" || roleArn != "" {
		t.Fatalf("AIStore must bypass per-workload IRSA; got tokenFile=%q roleArn=%q", tokenFile, roleArn)
	}
	if _, statErr := os.Stat(filepath.Join(configDir, webIdentityTokenFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("no token file should be written for AIStore; stat err = %v", statErr)
	}
}

func TestResolveWorkloadIdentity_NoneModeIsNoop(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	configDir := t.TempDir()
	// none mode never assumes a role, even if the kubelet supplied a token.
	volCtx := map[string]string{
		serviceAccountTokensVolCtxKey: `{"sts.amazonaws.com":{"token":"ignored"}}`,
	}
	tokenFile, roleArn, err := ns.resolveWorkloadIdentity(configDir, volCtx, credentialModeNone)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokenFile != "" || roleArn != "" {
		t.Fatalf("none mode must be a no-op; got tokenFile=%q roleArn=%q", tokenFile, roleArn)
	}
}

// --- helpers -----------------------------------------------------------------

func writeConfigOrFatal(t *testing.T, ns *nodeServer, volCtx, secrets map[string]string, mode credentialMode) (string, string) {
	t.Helper()
	dir, configPath, err := ns.writeConfig("/tmp/csi-target-test", volCtx, secrets, false, mode)
	if err != nil {
		t.Fatalf("writeConfig returned error: %v", err)
	}
	return dir, configPath
}

func readFileOrFatal(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func envHasPrefix(env []string, prefix string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

func envCount(env []string, prefix string) int {
	n := 0
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

func envValue(env []string, prefix string) string {
	val := ""
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			val = strings.TrimPrefix(e, prefix)
		}
	}
	return val
}
