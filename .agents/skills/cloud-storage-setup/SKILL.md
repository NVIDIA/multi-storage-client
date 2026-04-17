---
name: cloud-storage-setup
description: >-
  Set up MSC credentials and MCP server integration for cloud storage access.
  Use when the user wants to interact with cloud storage (S3, GCS, Azure, OCI),
  asks to set up credentials, connect to a bucket, list objects, move files, or
  sync data. Checks for existing config first — only runs interactive setup if
  credentials are missing.
invocable: auto
---

# Cloud Storage Setup

Set up Multi-Storage Client (MSC) credentials and configure the MCP server so the agent can interact with cloud storage natively. This skill checks for existing configuration first and only runs interactive setup when credentials are missing.

## When to Use

- User wants to interact with cloud storage (S3, GCS, Azure, OCI).
- User asks to "set up creds", "connect to S3", "find files in a bucket".
- User asks to "list objects", "move files", "sync data", "upload", "download".
- User asks about cloud storage and the MSC MCP server is not yet configured.

## Prerequisites

- MSC installed with MCP extra: `pip install 'multi-storage-client[mcp]'`
- `msc` command on PATH (verify with `which msc`)

---

## Step 1: Check for Existing MSC Config

Before doing anything interactive, check if an MSC config already exists.

Run these checks in order:

```bash
# 1. Check if msc is installed
which msc

# 2. Check env var
echo $MSC_CONFIG

# 3. Check standard discovery paths
ls -la ~/.msc_config.yaml ~/.msc_config.json 2>/dev/null
ls -la ~/.config/msc/config.yaml ~/.config/msc/config.json 2>/dev/null
ls -la ${XDG_CONFIG_HOME:-~/.config}/msc/config.yaml 2>/dev/null
ls -la /etc/msc_config.yaml /etc/msc_config.json 2>/dev/null

# 4. If any config found, validate it
msc config validate
```

**Decision tree:**

- **Config found and valid** -> Tell the user their config is ready. Skip to Step 3.
- **Config found but invalid** -> Show the validation error. Offer to fix it or create a new one. If user wants a fix, edit the existing file. Otherwise proceed to Step 2.
- **No config found** -> Proceed to Step 2.
- **`msc` not on PATH** -> Tell user to install MSC first: `pip install 'multi-storage-client[mcp]'`. Stop here.

---

## Step 2: Interactive Credential Setup (only if no config)

Ask the user these questions. Use the AskQuestion tool when available, otherwise ask in conversation.

### 2a. Cloud provider

Ask: Which cloud provider?

| Provider | MSC `type` | Common URL format |
|----------|-----------|-------------------|
| AWS S3 | `s3` | `s3://bucket/path` |
| Google Cloud Storage | `gcs` | `gs://bucket/path` |
| Azure Blob Storage | `azure` | `msc://profile/path` |
| Oracle Cloud (OCI) | `oci` | `msc://profile/path` |
| S3-compatible (MinIO, etc.) | `s3` | `s3://bucket/path` |
| Local filesystem | `file` | `/path/to/dir` |

### 2b. Bucket/container and region

Ask:
- Bucket or container name?
- Region? (e.g., `us-east-1` for AWS, not needed for GCS)
- Custom endpoint URL? (only for S3-compatible like MinIO)

### 2c. Credential method

Ask: Are you already authenticated via your cloud SDK?

- **AWS**: `aws configure` or `AWS_ACCESS_KEY_ID` env var set?
- **GCS**: `gcloud auth application-default login` done?
- **Azure**: `az login` done?
- **OCI**: `~/.oci/config` exists?

**If YES (SDK defaults)** — generate config WITHOUT a `credentials_provider` block. MSC will use the SDK's default credential chain automatically.

**If NO** — generate config with env var references and tell the user which environment variables to export. Never write raw secrets.

### 2d. Generate the config file

Choose a profile name based on the bucket (e.g., `my-s3-bucket`).

Write `~/.msc_config.yaml` using the appropriate template below.

**S3 with SDK defaults (recommended):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: s3
      options:
        base_path: <bucket-name>
        region_name: <region>
```

**S3 with explicit credentials (env var references):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: s3
      options:
        base_path: <bucket-name>
        region_name: <region>
    credentials_provider:
      type: S3Credentials
      options:
        access_key: ${AWS_ACCESS_KEY_ID}
        secret_key: ${AWS_SECRET_ACCESS_KEY}
```

**S3-compatible (MinIO, etc.):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: s3
      options:
        base_path: <bucket-name>
        endpoint_url: <endpoint-url>
        region_name: <region>
    credentials_provider:
      type: S3Credentials
      options:
        access_key: ${S3_ACCESS_KEY}
        secret_key: ${S3_SECRET_KEY}
```

**GCS with SDK defaults (recommended):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: gcs
      options:
        base_path: <bucket-name>
```

**GCS with service account:**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: gcs
      options:
        base_path: <bucket-name>
    credentials_provider:
      type: GoogleServiceAccountCredentialsProvider
      options:
        service_account_json_path: ${GOOGLE_APPLICATION_CREDENTIALS}
```

**Azure with DefaultAzureCredentials (recommended):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: azure
      options:
        base_path: <container-name>
        account_url: https://<storage-account>.blob.core.windows.net
    credentials_provider:
      type: DefaultAzureCredentials
```

**Azure with connection string:**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: azure
      options:
        base_path: <container-name>
    credentials_provider:
      type: AzureCredentials
      options:
        connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
```

**OCI (uses ~/.oci/config automatically):**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: oci
      options:
        base_path: <bucket-name>
        namespace: <namespace>
        region: <region>
```

**Local filesystem:**

```yaml
profiles:
  <profile-name>:
    storage_provider:
      type: file
      options:
        base_path: <absolute-path>
```

### 2e. Validate

```bash
msc config validate --config ~/.msc_config.yaml
```

If validation fails, read the error and fix the config. Common issues:
- Missing env vars referenced with `${}` — tell user which vars to export.
- Invalid YAML syntax — check indentation.
- Unknown provider type — check spelling.

---

## Step 3: Configure MCP Server (tool-agnostic)

### 3a. Detect the current coding tool

Check in this order:
1. If `.cursor/` exists in the workspace -> **Cursor**
2. If `CLAUDE.md` or `.claude/` exists in the workspace -> **Claude Code**
3. If `.windsurf/` exists in the workspace -> **Windsurf**
4. If none detected -> ask the user which tool they're using

### 3b. Check if MCP server is already configured

Read the tool's MCP config file (if it exists):

| Tool | MCP config location |
|------|-------------------|
| Cursor | `~/.cursor/mcp.json` (user) or `.cursor/mcp.json` (project) |
| Claude Code | `.mcp.json` (project root) or `~/.claude.json` (user) |
| Windsurf | `.windsurf/mcp.json` (project) or `~/.codeium/windsurf/mcp.json` (user) |

Look for an existing `"Multi-Storage Client"` or `"msc"` entry in `mcpServers`. If found, skip to Step 4.

### 3c. Write MCP server config

The JSON shape is the same for all tools. Only the file path differs.

**Without explicit config path** (MSC uses standard discovery):

```json
{
  "mcpServers": {
    "Multi-Storage Client": {
      "command": "msc",
      "args": ["mcp-server", "start"]
    }
  }
}
```

**With explicit config path** (if config is in a non-standard location):

```json
{
  "mcpServers": {
    "Multi-Storage Client": {
      "command": "msc",
      "args": ["mcp-server", "start", "--config", "/absolute/path/to/msc_config.yaml"]
    }
  }
}
```

If the MCP config file already exists with other servers, **merge** the new entry into the existing `mcpServers` object. Do not overwrite other server entries.

For **Windsurf**, the same JSON shape applies — write to `.windsurf/mcp.json` (project) or `~/.codeium/windsurf/mcp.json` (user).

### 3d. Notify the user

Tell the user:
- MCP server has been configured.
- They need to **restart their agent/tool** to pick up the new MCP config.
- After restart, the agent will have access to 10 cloud storage tools (list, info, upload, download, delete, copy, sync, etc.).

---

## Step 4: Verify Connectivity

After the user restarts their tool (or if MCP was already configured), verify the setup works.

**If MCP tools are available** (msc_list is callable):
- Call `msc_list` on the configured bucket root to confirm connectivity.

**If MCP tools are not yet available** (before restart):
- Use the CLI as a fallback:

```bash
msc ls msc://<profile-name>/
```

or with a provider URL:

```bash
msc ls s3://<bucket-name>/
```

Report the result to the user. If it fails:
- Check credentials: are env vars exported? Is SDK auth valid?
- Check network: can the machine reach the cloud provider?
- Check config: run `msc config validate` again.

---

## Quick Reference (Post-Setup)

Once the MCP server is configured and running, the agent has native access to these tools:

| Natural language request | MCP tool |
|--------------------------|----------|
| "List files at s3://bucket/path/" | `msc_list` |
| "What's in gs://bucket/media/" | `msc_list` |
| "Get info on this file" | `msc_info` |
| "Is this path a file or directory?" | `msc_is_file` |
| "Is this bucket empty?" | `msc_is_empty` |
| "Upload local file to cloud" | `msc_upload_file` |
| "Download file to local" | `msc_download_file` |
| "Copy file within same bucket" | `msc_copy` |
| "Move/sync files between locations" | `msc_sync` |
| "Delete old files" | `msc_delete` (with `recursive` for directories) |
| "Sync to replicas" | `msc_sync_replicas` |

URL formats:
- `msc://profile-name/path/to/object` — uses profiles from MSC config
- `s3://bucket/path` — AWS S3 (implicit profile)
- `gs://bucket/path` — Google Cloud Storage (implicit profile)

## Security Reminders

- **Never write raw credentials** to config files. Always use env var references (`${VAR}`).
- **Never commit credential files**. `~/.msc_config.yaml` lives in the home directory, not the repo.
- **Prefer SDK default chains** (aws configure, gcloud auth, az login) over explicit keys.
- If the user tries to paste raw keys, warn them and suggest env vars instead.
