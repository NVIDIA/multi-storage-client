# Multi-Storage Client

The Multi-Storage Client (MSC) is a unified high-performance Python client for object and file stores such as AWS S3, Azure Blob Storage, Google Cloud Storage (GCS), NVIDIA AIStore, Oracle Cloud Infrastructure (OCI) Object Storage, POSIX file systems, and more.

It provides a generic interface to interact with objects and files across various storage services. This lets you spend less time learning each storage service's unique interface and lets you change where data is stored without having to change how your code accesses it.

## Getting Started

See the [documentation](https://nvidia.github.io/multi-storage-client/) to get started.

## Contribution Guidelines

- Start here: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`

### Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # Release notes.
├── .release_notes
│   └── ...
│
│   # Client Python package.
├── multi-storage-client
│   └── ...
│
│   # Client documentation.
├── multi-storage-client-docs
│   └── ...
│
│   # Internal helper scripts.
├── multi-storage-client-scripts
│   └── ...
│
│   # Client web UI.
├── multi-storage-explorer
│   └── ...
│
│   # File system Go package.
├── multi-storage-file-system
│   └── ...
│
│   # Nix flake outputs.
├── nix
│   └── ...
│
│   # GitLab pipeline entrypoint.
├── .gitlab-ci.yml
│
│   # Nix configuration.
├── flake.nix
├── flake.lock 🤖
│
│   # Python configuration.
├── pyproject.toml
├── uv.lock 🤖
│
│   # Build recipes.
└── justfile
```

### Tools

Nix (required) and direnv (optional, but strongly recommended for shell + editor integration) are used for development.

We rely on many tools which aren't vended as Python packages (e.g. storage emulators, compiler toolchains) but are available as Nix packages. While these can be installed individually, the toolset and exact versions will change over time. This is captured by the Nix shell described by the project's Nix flake.

#### Nix

[Nix](https://nixos.org) is a package manager and build system centered around reproducibility.

For us, Nix's most useful feature is its ability to create reproducible + isolated CLI shells on the same machine which use different versions of the same package (e.g. Java 17 and 21). Shell configurations can be encapsulated in Nix files which can be shared across multiple computers.

The best way to install Nix is with the [Determinate Nix Installer](https://github.com/DeterminateSystems/nix-installer) ([guide](https://zero-to-nix.com/start/install)).

Once installed, running `nix develop` in a directory with a `flake.nix` will create a nested Bash shell defined by the flake.

> 🔖
>
> If you're on a network with lots of GitHub traffic, you may get a rate limiting error. To work around this, you can either switch networks (e.g. turn off VPN) or add a GitHub personal access token (classic) to your [Nix configuration](https://nix.dev/manual/nix/latest/command-ref/conf-file).
>
> ```text
> access-tokens = github.com=ghp_{rest of token}
> ```

#### direnv

[direnv](https://direnv.net) is a shell extension which can automatically load and unload environment variables when you enter or leave a specific directory.

It can automatically load and unload a Nix environment when we enter and leave a project directory.

**Unlike `nix develop` which drops you in a nested Bash shell, direnv extracts the environment variables from the nested Bash shell into your current shell (e.g. Bash, Zsh, Fish).**

Follow the [installation instructions on its website](https://direnv.net#basic-installation).

It also has [editor integration](https://github.com/direnv/direnv/wiki#editor-integration). Note that some integrations won't automatically reload the environment after Nix flake changes unlike direnv itself so manual reloads may be needed.

### Developing

Common recipes are provided as Just recipes. To list them, run:

```shell
just
```

#### Building the Project

To do a full release build, run:

```shell
just build
```

If you want to use a specific Python binary such as Python 3.10, run:

```shell
just python-binary=python3.10 build
```

### Security

- Vulnerability disclosure: `SECURITY.md`
- Do not file public issues for security reports.

### Support

- Level: Experimental
- How to get help: Issues/Discussions

## License

This project is licensed under the Apache-2.0 License - see the `LICENSE.md` file for details.
