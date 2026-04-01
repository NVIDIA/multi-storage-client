# Nix

Nix flake outputs.

## Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # Nixpkgs overlays.
├── overlays
│   └── {overlay}
│       └── multi-storage-client
│           │   # Packages.
│           ├── packages
│           │   └── {package}
│           │       ├── package.nix
│           │       └── {package support file (e.g. patch)}
│           │
│           │   # Development shells.
│           └── devShells
│               └── {shell}.nix
│
│   # Build recipes.
└── justfile
```
