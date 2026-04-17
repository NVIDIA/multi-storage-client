#
# Just configuration.
#
# https://just.systems/man/en
#

# Default to the first Python binary on `PATH`.
python-binary := "python"

# List recipes.
help:
    just --list

# Build nix.
nix:
    just nix/build

# Build multi-storage-explorer.
multi-storage-explorer:
    just multi-storage-explorer/build

# Build multi-storage-client.
multi-storage-client: multi-storage-explorer
    just python-binary={{python-binary}} multi-storage-client/build

# Build multi-storage-client-docs.
multi-storage-client-docs: multi-storage-client
    just python-binary={{python-binary}} multi-storage-client-docs/build

# Build multi-storage-client-scripts.
multi-storage-client-scripts:
    just python-binary={{python-binary}} multi-storage-client-scripts/build

# Build multi-storage-file-system.
multi-storage-file-system:
    just multi-storage-file-system/build

# Release build.
build: nix multi-storage-explorer multi-storage-client multi-storage-client-docs multi-storage-client-scripts multi-storage-file-system

# Generate tool-specific rules and MCP config from .agents/ (canonical source).
# Skills in .agents/skills/ are discovered natively by most tools (Cursor,
# Windsurf, Copilot, Codex). This recipe only generates rules (which need
# per-tool frontmatter), MCP server config, and Windsurf workflows.
# Re-run after editing .agents/rules/ to sync changes.
setup-agent-tools *flags:
    #!/usr/bin/env bash
    set -euo pipefail

    RULE_SOURCE=".agents/rules"

    # --- Shared files ---
    if [[ ! -f AGENTS.md ]]; then
        echo "WARNING: AGENTS.md not found at repo root — create it before running this."
    else
        echo "OK: AGENTS.md exists"
    fi
    if [[ ! -f agent-learnings.md ]]; then
        echo "WARNING: agent-learnings.md not found at repo root — create it before running this."
    else
        echo "OK: agent-learnings.md exists"
    fi
    if [[ ! -d "${RULE_SOURCE}" ]]; then
        echo "ERROR: ${RULE_SOURCE} not found — cannot generate tool configs."
        exit 1
    fi

    # --- Find msc binary ---
    MSC_CMD=""
    if command -v msc &>/dev/null; then
        MSC_CMD="$(command -v msc)"
        echo "OK: msc on PATH (${MSC_CMD})"
    elif [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/msc" ]]; then
        MSC_CMD="${VIRTUAL_ENV}/bin/msc"
        echo "OK: msc in active venv (${MSC_CMD})"
    elif [[ -x "$(pwd)/.venv/bin/msc" ]]; then
        MSC_CMD="$(pwd)/.venv/bin/msc"
        echo "OK: msc in project .venv (${MSC_CMD})"
    elif [[ -x "$(pwd)/multi-storage-client/.venv/bin/msc" ]]; then
        MSC_CMD="$(pwd)/multi-storage-client/.venv/bin/msc"
        echo "OK: msc in multi-storage-client/.venv (${MSC_CMD})"
    else
        echo "WARNING: msc not found — MCP server config will be skipped"
        echo "  Install with: pip install 'multi-storage-client[mcp]'"
    fi

    # Verify Python >= 3.10 (required by fastmcp, the MCP server dependency)
    if [[ -n "${MSC_CMD}" ]]; then
        MSC_PYTHON="$(dirname "${MSC_CMD}")/python3"
        if [[ -x "${MSC_PYTHON}" ]]; then
            PY_VERSION=$("${MSC_PYTHON}" -c "import sys; print(f'{sys.version_info.minor}')" 2>/dev/null || echo "0")
            if [[ "${PY_VERSION}" -lt 10 ]]; then
                echo "WARNING: MCP server requires Python >= 3.10 (found 3.${PY_VERSION})"
                echo "  Rebuild venv with: python3.10 -m venv .venv && .venv/bin/pip install 'multi-storage-client[mcp]'"
                MSC_CMD=""
            fi
        fi
    fi

    configure_mcp() {
        local config_file="$1"
        local msc_command="$2"
        if [[ -z "${msc_command}" ]]; then
            echo "  MCP: skipped (msc not found)"
            return
        fi
        python3 .agents/configure-mcp.py "${config_file}" "${msc_command}"
    }

    # Windsurf workflows: copy prepare-mr skill as a workflow.
    copy_windsurf_workflows() {
        local skill_source=".agents/skills"
        if [[ -d "${skill_source}/prepare-mr" ]]; then
            mkdir -p .windsurf/workflows
            cp "${skill_source}/prepare-mr/SKILL.md" .windsurf/workflows/prepare-mr.md
            echo "  copied workflow: prepare-mr"
        fi
    }

    mcp_config_path() {
        case "$1" in
            cursor)    echo "${HOME}/.cursor/mcp.json" ;;
            claude)    echo ".mcp.json" ;;
            windsurf)  echo ".windsurf/mcp.json" ;;
        esac
    }

    DETECTED_TOOLS=()

    if [[ -d .cursor ]] || [[ "{{ flags }}" == "--all" ]]; then
        DETECTED_TOOLS+=(cursor)
        echo ""
        echo "=== Detected: Cursor (.cursor/) ==="
        mkdir -p .cursor/rules

        {
            echo "---"
            echo "alwaysApply: true"
            echo "description: Loads shared agent learnings at session start."
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/agent-learnings.md"
        } > .cursor/rules/agent-learnings.mdc

        {
            echo "---"
            echo "alwaysApply: true"
            echo "description: Shared project workflow rules for multi-storage-client. References AGENTS.md for project guidance."
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/project-rules.md"
        } > .cursor/rules/project-rules.mdc
        echo "  wrote rules (agent-learnings.mdc, project-rules.mdc)"
        echo "  skills: discovered natively from .agents/skills/"

        configure_mcp "$(mcp_config_path cursor)" "${MSC_CMD}"
    fi

    if [[ -d .claude ]] || [[ -f CLAUDE.md ]] || [[ "{{ flags }}" == "--all" ]]; then
        DETECTED_TOOLS+=(claude)
        echo ""
        echo "=== Detected: Claude Code (.claude/) ==="
        mkdir -p .claude/rules

        {
            echo "# MSC — Claude Code Configuration"
            echo ""
            echo "Read these files at the start of every session:"
            echo ""
            echo "- \`AGENTS.md\` — project structure, architecture, conventions, commands, cross-language sync points."
            echo "- \`agent-learnings.md\` — accumulated mistakes and corrections. Follow every entry."
            echo ""
            echo "For implementation workflow and testing policy, see \`.claude/rules/\`."
        } > CLAUDE.md
        echo "  wrote CLAUDE.md"

        {
            echo "---"
            echo "description: Loads shared agent learnings at session start."
            echo "alwaysApply: true"
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/agent-learnings.md"
        } > .claude/rules/agent-learnings.md

        {
            echo "---"
            echo "description: Shared project workflow rules for multi-storage-client. References AGENTS.md for project guidance."
            echo "alwaysApply: true"
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/project-rules.md"
        } > .claude/rules/project-rules.md
        echo "  wrote rules (agent-learnings.md, project-rules.md)"

        rm -rf .claude/skills
        cp -r .agents/skills .claude/skills
        echo "  skills: copied from .agents/skills/ (Claude Code does not discover .agents/skills/ natively)"

        configure_mcp "$(mcp_config_path claude)" "${MSC_CMD}"
    fi

    if [[ -d .windsurf ]] || [[ "{{ flags }}" == "--all" ]]; then
        DETECTED_TOOLS+=(windsurf)
        echo ""
        echo "=== Detected: Windsurf (.windsurf/) ==="
        mkdir -p .windsurf/rules

        {
            echo "---"
            echo "description: Loads shared agent learnings at session start."
            echo "alwaysApply: true"
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/agent-learnings.md"
        } > .windsurf/rules/agent-learnings.md

        {
            echo "---"
            echo "description: Shared project workflow rules for multi-storage-client. References AGENTS.md for project guidance."
            echo "alwaysApply: true"
            echo "---"
            echo ""
            cat "${RULE_SOURCE}/project-rules.md"
        } > .windsurf/rules/project-rules.md
        echo "  wrote rules (agent-learnings.md, project-rules.md)"
        echo "  skills: discovered natively from .agents/skills/"

        copy_windsurf_workflows
        configure_mcp "$(mcp_config_path windsurf)" "${MSC_CMD}"
    fi

    if [[ ${#DETECTED_TOOLS[@]} -eq 0 ]]; then
        echo ""
        echo "No tools detected. Create a tool directory first (.cursor/, .claude/, .windsurf/) or pass --all."
    else
        echo ""
        echo "Done. Set up: ${DETECTED_TOOLS[*]}"
        echo "Restart your tool to activate the MCP server."
    fi
