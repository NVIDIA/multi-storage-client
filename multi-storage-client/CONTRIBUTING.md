# Contributing

## Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # Build artifacts.
├── dist 🤖
│   └── ...
│
│   # Source.
├── src
│   └── ...
│
│   # Test source.
├── tests
│   └── test_multistorageclient
│       │   # Unit tests.
│       ├── unit
│       │   └── ...
│       │
│       │   # Load tests.
│       ├── load
│       │   └── ...
│       │
│       │   # End-to-end (E2E) tests.
│       └── e2e
│           └── ...
│
│   # Build recipes.
├── justfile
│
│   # Python package configuration.
└── pyproject.toml
```
