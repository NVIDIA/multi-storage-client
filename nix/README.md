# Nix

Nix flake outputs.

## Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # Library.
├── lib
│   ├── {attribute}.nix
│   └── {namespace}
│       ├── {attribute}.nix
│       └── ...
│
│   # Packages.
├── packages
│   └── {package}
│       ├── package.nix
│       └── {package support file (e.g. patch)}
│
│   # Development shells.
├── devShells
│   └── {shell}.nix
│
│   # NixOS modules.
├── nixosModules
│   └── {module}.nix
│
│   # system-manager configurations.
└── systemConfigs
    └── {configuration}.nix
```
