# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import itertools
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.shortcuts import _resolve_msc_url

from .models import (
    ConfigUploadResponse,
    CopyRequest,
    DeleteRequest,
    DownloadRequest,
    InfoRequest,
    ListRequest,
    PreviewRequest,
    SyncRequest,
)

# Get the package root directory
PACKAGE_ROOT = Path(__file__).parent.parent
STATIC_DIR = (PACKAGE_ROOT / "static").resolve()

app = FastAPI(
    title="MSC Explorer API", description="RESTful API for Multi-Storage Client file operations", version="1.0.0"
)


# Configure CORS
# CORS is always enabled to support:
# 1. Local development with separate frontend dev server
# 2. Cloud deployments where frontend may be served from CDN
# 3. Embedding the UI in other applications
#
# By default, allows common localhost ports. Set CORS_ORIGINS env var for custom origins.
# Example: CORS_ORIGINS="https://myapp.com,https://cdn.myapp.com"
def get_cors_origins() -> list[str]:
    """
    Get CORS allowed origins.

    Returns origins from CORS_ORIGINS env var (comma-separated),
    or defaults to common localhost development ports.
    """
    custom_origins = os.getenv("CORS_ORIGINS")
    if custom_origins:
        # Parse comma-separated origins, strip whitespace
        origins = [origin.strip() for origin in custom_origins.split(",") if origin.strip()]
        if origins:
            return origins

    # Default: allow common localhost ports for development
    return [
        "http://localhost:5173",  # Vite default
        "http://localhost:5174",  # Vite alternate
        "http://localhost:5175",  # Vite alternate
        "http://localhost:3000",  # Common React port
        "http://localhost:8080",  # Common alt port
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://127.0.0.1:5175",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
    ]


cors_origins = get_cors_origins()

# Always enable CORS middleware
# When frontend is served from the same origin (production), CORS headers are simply ignored.
# When frontend is served from a different origin (development/CDN), CORS headers allow the request.
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Custom exception handler to ensure all errors are properly formatted for the frontend
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler to catch any unhandled exceptions and return them
    in a consistent format for the frontend to display.
    """
    # If it's already an HTTPException, let FastAPI handle it normally
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    # For other exceptions, log them and return a user-friendly error
    import traceback

    error_trace = traceback.format_exc()
    print(f"Unhandled exception: {error_trace}")

    # Determine error type and provide appropriate message
    error_msg = str(exc)

    # Common error patterns and their user-friendly messages
    if "permission" in error_msg.lower() or "forbidden" in error_msg.lower():
        status_code = 403
        user_msg = f"Permission denied: {error_msg}"
    elif "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
        status_code = 404
        user_msg = f"Resource not found: {error_msg}"
    elif "timeout" in error_msg.lower():
        status_code = 504
        user_msg = f"Operation timed out: {error_msg}"
    elif "invalid" in error_msg.lower() or "bad" in error_msg.lower():
        status_code = 400
        user_msg = f"Invalid request: {error_msg}"
    else:
        status_code = 500
        user_msg = f"An error occurred: {error_msg}"

    return JSONResponse(status_code=status_code, content={"detail": user_msg})


# Global state
msc_config: Optional[Dict[str, Any]] = None
executor = ThreadPoolExecutor(max_workers=4)

# Cache for StorageClient instances per profile
_client_cache: Dict[str, Any] = {}


def get_msc_client_and_path(url: str) -> tuple[Any, str]:
    """Get or create the MSC client instance for the given URL's profile and extract the path"""
    if msc_config is None:
        raise HTTPException(
            status_code=400, detail="MSC configuration not loaded. Please upload a configuration first."
        )

    if not url.startswith("msc://"):
        raise HTTPException(status_code=400, detail="URL must start with msc://")

    profile, path = _resolve_msc_url(url)

    if profile not in msc_config.get("profiles", {}):
        raise HTTPException(status_code=400, detail=f"Profile '{profile}' not found in configuration")

    # Check if we already have a client for this profile
    if profile not in _client_cache:
        # Create new StorageClient for this profile
        if StorageClientConfig is None or StorageClient is None:
            raise HTTPException(status_code=503, detail="MSC not available. Running in demo mode.")
        try:
            config_json = json.dumps(msc_config)
            # StorageClientConfig and StorageClient are guaranteed non-None after the check above
            storage_config = StorageClientConfig.from_json(config_json, profile=profile)  # pyright: ignore[reportOptionalMemberAccess]
            _client_cache[profile] = StorageClient(storage_config)  # pyright: ignore[reportOptionalCall]
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to create StorageClient for profile '{profile}': {str(e)}"
            )

    return _client_cache[profile], path


def run_sync(func, *args, **kwargs):
    """Run synchronous MSC operations in thread pool"""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(executor, func, *args, **kwargs)


@app.get("/api/health")
async def health():
    """API health check endpoint"""
    return {
        "status": "healthy",
        "config_loaded": msc_config is not None,
        "frontend_available": _static_assets_available,
    }


@app.post("/api/config/upload", response_model=ConfigUploadResponse)
async def upload_config(config_file: UploadFile = File(...)):
    """
    Upload MSC configuration file

    The configuration should be in YAML or JSON format compatible with MSC.
    """
    global msc_config

    try:
        # Read the uploaded file
        content = await config_file.read()

        # Try to parse as JSON first
        assert config_file.filename is not None

        if config_file.filename.endswith(".json"):
            config = json.loads(content)
        elif config_file.filename.endswith(".yaml") or config_file.filename.endswith(".yml"):
            config = yaml.safe_load(content)
        else:
            raise HTTPException(status_code=400, detail="Invalid file type. Please upload a JSON or YAML file.")

        # Save config and clear client cache
        msc_config = config
        _client_cache.clear()

        # Initialize MSC client with the config
        profiles = list(config.get("profiles", {}).keys())

        return ConfigUploadResponse(
            status="success",
            message=f"Configuration loaded successfully. Found {len(profiles)} profile(s).",
            profiles=profiles,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load configuration: {str(e)}")


@app.get("/api/config/profiles")
async def get_profiles():
    """
    Get list of available MSC profiles
    """
    if msc_config is None:
        raise HTTPException(status_code=400, detail="No configuration loaded. Please upload a config file first.")

    profiles = list(msc_config.get("profiles", {}).keys())
    return {"profiles": profiles, "count": len(profiles)}


@app.post("/api/files/list")
async def list_files(request: ListRequest):
    """
    List files and directories at the given URL

    Calls MSC's list() method
    """
    client, path = get_msc_client_and_path(request.url)

    try:
        # Run MSC list operation in thread pool
        def do_list():
            # Get iterator from MSC
            iterator = client.list(
                path=path,
                start_after=request.start_after,
                end_at=request.end_at,
                include_directories=request.include_directories,
                include_url_prefix=False,
            )

            # Only consume up to limit items from iterator to avoid loading all objects
            if request.limit:
                results = list(itertools.islice(iterator, request.limit))
            else:
                results = list(iterator)

            return results

        result = await run_sync(do_list)

        # Format the result for frontend consumption
        items = []
        for item in result:
            # item is an ObjectMetadata instance
            # Extract name from key (last part of path)
            name = item.key.split("/")[-1] if "/" in item.key else item.key
            # Remove trailing slash from directory names
            if name.endswith("/"):
                name = name[:-1]

            items.append(
                {
                    "name": name or item.key,
                    "key": item.key,
                    "type": item.type,  # "file" or "directory"
                    "size": item.content_length or 0,
                    "last_modified": item.last_modified.isoformat() if item.last_modified else "",
                    "is_directory": item.type == "directory",
                }
            )

        return {"items": items, "count": len(items)}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Directory not found: {request.url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.post("/api/files/info")
async def get_file_info(request: InfoRequest):
    """
    Get information about a file or directory

    Calls MSC's info() method
    """
    client, path = get_msc_client_and_path(request.url)

    try:

        def do_info():
            return client.info(path=path)

        metadata = await run_sync(do_info)

        # Convert ObjectMetadata to dict for JSON serialization
        result = {
            "key": metadata.key,
            "content_length": metadata.content_length,
            "last_modified": metadata.last_modified.isoformat() if metadata.last_modified else None,
            "type": metadata.type,
            "content_type": metadata.content_type,
            "etag": metadata.etag if hasattr(metadata, "etag") else None,
            "storage_class": metadata.storage_class if hasattr(metadata, "storage_class") else None,
            "metadata": metadata.metadata if hasattr(metadata, "metadata") else None,
        }

        return result

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {request.url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.post("/api/files/preview")
async def preview_file(request: PreviewRequest):
    """
    Preview a file's content and metadata

    Reads the file content (up to max_bytes) and returns it along with metadata.
    For files larger than max_bytes, reads and previews the first max_bytes.
    Supports text files, images, and other common file types.
    """
    client, path = get_msc_client_and_path(request.url)

    try:
        # Get file metadata first
        def do_info():
            return client.info(path=path)

        metadata = await run_sync(do_info)

        # Convert metadata to dict
        file_info = {
            "name": request.url.split("/")[-1],
            "size": metadata.content_length or 0,
            "last_modified": metadata.last_modified.isoformat() if metadata.last_modified else "",
            "type": metadata.type,
            "content_type": (
                metadata.content_type
                if hasattr(metadata, "content_type") and metadata.content_type
                else "application/octet-stream"
            ),
            "etag": metadata.etag if hasattr(metadata, "etag") else None,
        }

        # Determine if content is truncated
        file_size = file_info["size"]
        content_truncated = file_size > request.max_bytes

        content = None
        is_text = False
        is_image = False
        can_preview = True

        if file_size > 0:
            # Read file content using MSC's read() method with byte_range
            # For large files, only read the first max_bytes
            max_bytes = request.max_bytes

            def do_read():
                if content_truncated:
                    # Use byte_range to read only first max_bytes
                    # Range is a dict with 'start' and 'end' keys
                    byte_range = {"start": 0, "end": max_bytes - 1}
                    return client.read(path=path, byte_range=byte_range)
                else:
                    return client.read(path=path)

            try:
                file_bytes = await run_sync(do_read)
            except Exception as e:
                # If byte_range isn't supported, fall back to reading full file and truncating
                try:

                    def do_read_full():
                        return client.read(path=path)

                    file_bytes = await run_sync(do_read_full)
                    if len(file_bytes) > max_bytes:
                        file_bytes = file_bytes[:max_bytes]
                except Exception:
                    raise e

            # Determine file type based on content_type and extension
            content_type = (file_info.get("content_type") or "").lower()
            file_ext = file_info["name"].lower().split(".")[-1] if "." in file_info["name"] else ""

            # Check if it's an image
            if content_type.startswith("image/") or file_ext in ["png", "jpg", "jpeg", "gif", "bmp", "svg", "webp"]:
                is_image = True
                # Encode image as base64
                import base64

                content = base64.b64encode(file_bytes).decode("utf-8")
                if not content_type.startswith("image/"):
                    # Infer content type from extension
                    content_type_map = {
                        "png": "image/png",
                        "jpg": "image/jpeg",
                        "jpeg": "image/jpeg",
                        "gif": "image/gif",
                        "bmp": "image/bmp",
                        "svg": "image/svg+xml",
                        "webp": "image/webp",
                    }
                    file_info["content_type"] = content_type_map.get(file_ext, "image/png")

            # Check if it's a text file
            elif (
                content_type.startswith("text/")
                or content_type in ["application/json", "application/xml", "application/javascript"]
                or file_ext
                in [
                    "txt",
                    "md",
                    "py",
                    "js",
                    "jsx",
                    "ts",
                    "tsx",
                    "json",
                    "xml",
                    "yaml",
                    "yml",
                    "html",
                    "css",
                    "sh",
                    "bash",
                    "c",
                    "cpp",
                    "h",
                    "java",
                    "rs",
                    "go",
                    "rb",
                    "php",
                    "sql",
                    "csv",
                    "log",
                    "ini",
                    "cfg",
                    "conf",
                    "toml",
                ]
            ):
                is_text = True
                # Try to decode as UTF-8
                try:
                    content = file_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    # If can't decode as UTF-8, it's binary
                    is_text = False
                    content = None

        return {
            "file_info": file_info,
            "content": content,
            "content_truncated": content_truncated,
            "is_text": is_text,
            "is_image": is_image,
            "can_preview": can_preview,
            "preview_message": (
                "Preview not available for this file type" if content is None and not is_text and not is_image else None
            ),
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {request.url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception:
        # Re-raise to let global exception handler provide user-friendly message
        raise


@app.post("/api/files/upload")
async def upload_file(url: str = Form(...), file: UploadFile = File(...)):
    """
    Upload a file to the specified URL

    Calls MSC's upload_file() method
    """
    client, path = get_msc_client_and_path(url)

    # Create a temporary file to store the upload
    temp_file = None
    try:
        # Save uploaded file to temp location
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_file = temp.name
            content = await file.read()
            temp.write(content)

        # Upload using MSC
        def do_upload():
            return client.upload_file(remote_path=path, local_path=temp_file)

        result = await run_sync(do_upload)

        return {"status": "success", "message": f"File '{file.filename}' uploaded successfully", "result": result}

    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception:
        # Re-raise to let global exception handler provide user-friendly message
        raise

    finally:
        # Clean up temp file
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)


@app.post("/api/files/download")
async def download_file(request: DownloadRequest):
    """
    Download a file from the specified URL

    Calls MSC's download_file() method
    """
    client, path = get_msc_client_and_path(request.url)

    # Create a temporary file for download
    temp_file = None

    try:
        # Create temp file
        temp_fd, temp_file = tempfile.mkstemp()
        os.close(temp_fd)

        # Download using MSC
        def do_download():
            return client.download_file(remote_path=path, local_path=temp_file)

        await run_sync(do_download)

        # Extract filename from URL
        filename = request.url.split("/")[-1] or "download"

        # Return file as response
        return FileResponse(
            temp_file,
            media_type="application/octet-stream",
            filename=filename,
            background=None,  # We'll handle cleanup differently
        )
    except FileNotFoundError:
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
        raise HTTPException(status_code=404, detail=f"File not found: {request.url}")
    except PermissionError as e:
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception:
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
        # Re-raise to let global exception handler provide user-friendly message
        raise


@app.post("/api/files/delete")
async def delete_file(request: DeleteRequest):
    """
    Delete a file or directory

    Calls MSC's delete_object() method
    """
    client, path = get_msc_client_and_path(request.url)

    try:

        def do_delete():
            return client.delete(path=path, recursive=request.recursive)

        result = await run_sync(do_delete)

        return {"status": "success", "message": f"Successfully deleted '{request.url}'", "result": result}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File or directory not found: {request.url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception:
        # Re-raise to let global exception handler provide user-friendly message
        raise


@app.post("/api/files/copy")
async def copy_file(request: CopyRequest):
    """
    Copy a file from source to target URL

    Calls MSC's copy() method
    """
    source_client, source_path = get_msc_client_and_path(request.source_url)
    target_client, target_path = get_msc_client_and_path(request.target_url)

    try:

        def do_copy():
            # Check if same profile
            if source_client != target_client:
                raise ValueError("Cross-profile copy not supported via copy(). Use sync() instead.")
            return source_client.copy(src_path=source_path, dest_path=target_path)

        result = await run_sync(do_copy)

        return {
            "status": "success",
            "message": f"Successfully copied from '{request.source_url}' to '{request.target_url}'",
            "result": result,
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Source file or directory not found: {request.source_url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        # Re-raise to let global exception handler provide user-friendly message
        raise


@app.post("/api/files/sync")
async def sync_files(request: SyncRequest):
    """
    Sync files from source to target URL

    Calls MSC's sync_from() method
    """
    source_client, source_path = get_msc_client_and_path(request.source_url)
    target_client, target_path = get_msc_client_and_path(request.target_url)

    try:

        def do_sync():
            return target_client.sync_from(
                source_client=source_client,
                source_path=source_path,
                target_path=target_path,
                delete_unmatched_files=request.delete_unmatched_files,
                preserve_source_attributes=request.preserve_source_attributes,
            )

        result = await run_sync(do_sync)

        return {
            "status": "success",
            "message": f"Successfully synced from '{request.source_url}' to '{request.target_url}'",
            "result": result,
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Source directory not found: {request.source_url}")
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=f"Permission denied: {str(e)}")
    except Exception:
        # Re-raise to let global exception handler provide user-friendly message
        raise


# Check if static assets exist and mount them
_static_assets_available = (STATIC_DIR / "assets").exists() and (STATIC_DIR / "index.html").exists()

if _static_assets_available:
    # Mount static assets directory for JS/CSS bundles
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")


# HTML page shown when frontend hasn't been built yet
FRONTEND_NOT_BUILT_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MSC Explorer - Frontend Not Built</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e4e4e7;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }
        .container {
            max-width: 600px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 12px;
            padding: 2.5rem;
            text-align: center;
        }
        h1 {
            font-size: 1.75rem;
            margin-bottom: 1rem;
            color: #f4f4f5;
        }
        .icon {
            font-size: 3rem;
            margin-bottom: 1.5rem;
        }
        p {
            color: #a1a1aa;
            line-height: 1.6;
            margin-bottom: 1.5rem;
        }
        .code-block {
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 8px;
            padding: 1rem;
            text-align: left;
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
            font-size: 0.875rem;
            color: #22d3ee;
            overflow-x: auto;
        }
        .code-block .comment { color: #6b7280; }
        .api-note {
            margin-top: 1.5rem;
            padding-top: 1.5rem;
            border-top: 1px solid rgba(255,255,255,0.1);
            font-size: 0.875rem;
            color: #71717a;
        }
        a { color: #22d3ee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">🔧</div>
        <h1>Frontend Assets Not Found</h1>
        <p>
            The MSC Explorer frontend has not been built yet.
            To build the static assets, run:
        </p>
        <div class="code-block">
            <span class="comment"># Navigate to frontend directory and build</span><br>
            cd multi-storage-explorer && npm install && npm run build
        </div>
        <p class="api-note">
            The API is operational. Access the
            <a href="/docs">API documentation</a>
            or <a href="/api/health">health endpoint</a>.
        </p>
    </div>
</body>
</html>
"""


@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    """
    Serve the frontend application for all non-API routes.

    This catch-all route serves:
    - Static files if they exist (index.html, favicon, etc.)
    - The SPA index.html for client-side routing
    - A helpful message if the frontend hasn't been built
    """
    # Don't intercept API routes (they should 404 if not found)
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API endpoint not found")

    # If static assets are available, try to serve them
    if _static_assets_available:
        # Try to serve the exact file requested
        file_path = (STATIC_DIR / full_path).resolve()

        # Security: Ensure the resolved path is within STATIC_DIR (prevent path traversal)
        try:
            file_path.relative_to(STATIC_DIR)
        except ValueError:
            # Path is outside STATIC_DIR - potential path traversal attack
            raise HTTPException(status_code=404, detail="File not found")

        if file_path.is_file():
            return FileResponse(file_path)

        # For all other routes, serve index.html (SPA client-side routing)
        index_path = (STATIC_DIR / "index.html").resolve()

        # Security: Validate index.html path (should always pass, but be safe)
        try:
            index_path.relative_to(STATIC_DIR)
        except ValueError:
            raise HTTPException(status_code=404, detail="File not found")

        return FileResponse(index_path)

    # Frontend not built - return helpful HTML page
    return HTMLResponse(content=FRONTEND_NOT_BUILT_HTML, status_code=200)
