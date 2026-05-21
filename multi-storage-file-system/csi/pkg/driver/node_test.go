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

// --- buildEnv content per mode -----------------------------------------------

func TestBuildEnv_StaticIncludesAwsEnvVars(t *testing.T) {
	ns := newNodeServer("node-test", "/usr/local/bin/msfs")
	env := ns.buildEnv(map[string]string{
		"access_key_id":     "AKIA...",
		"secret_access_key": "secret",
		"session_token":     "session-token",
	}, credentialModeStatic)
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
	env := ns.buildEnv(map[string]string{
		"access_key_id":     "AKIA-stale",
		"secret_access_key": "stale-secret",
	}, credentialModeIRSA)
	for _, prefix := range []string{"AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN="} {
		if envHasPrefix(env, prefix) {
			t.Errorf("IRSA mode must NOT inject %s; got env %v", prefix, env)
		}
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
