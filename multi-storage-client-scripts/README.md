# Multi-Storage Client Scripts

Internal helper scripts for things too painful to do with Bash.

## Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # Source.
├── src
│   └── multistorageclient_scripts
│       │   # Command.
│       ├── cli
│       │   │   # Subcommand.
│       │   ├── {subcommand}
│       │   │   │   # Subcommand source.
│       │   │   ├── __init__.py
│       │   │   │
│       │   │   └── ...
│       │   │
│       │   │   # Command source.
│       │   ├── __init__.py
│       │   │
│       │   │   # Entrypoint.
│       │   └── __main__.py
│       │
│       │   # Utilities.
│       └── utils
│           │   # argparse extensions.
│           └── argparse_extensions
│               └── ...
│
│   # Python package configuration.
├── pyproject.toml
│
│   # Build recipes.
└── justfile
```
