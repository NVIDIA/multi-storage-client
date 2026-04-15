{
  actionlint,
  awscli2,
  azurite,
  bun,
  coreutils,
  curl,
  dpkg,
  fake-gcs-server,
  gettext,
  gh,
  git,
  git-lfs,
  gnused,
  gnutar,
  go_1_26,
  google-cloud-sdk,
  golangci-lint,
  grafana,
  jfrog-cli,
  jq,
  just,
  lib,
  lsof,
  mimir,
  mkShell,
  multi-storage-client,
  netcat-gnu,
  nix,
  nixfmt,
  nodejs-slim,
  openbao,
  openssh,
  openssl,
  pyright,
  python310,
  pythonInterpreter ? python310,
  rpm,
  ruff,
  rustup,
  stdenv,
  teleport,
  tempo,
  treefmt,
  util-linux,
  uv,
  versitygw,
  zip,
  zlib,
}:
# https://nixos.org/manual/nixpkgs/unstable#sec-pkgs-mkShell
mkShell {
  packages = [
    # Nix.
    #
    # Nix is dynamically linked on some systems. If we set LD_LIBRARY_PATH,
    # running Nix commands with the system-installed Nix may fail due to mismatched library versions.
    nix
    nixfmt
    # Utilities.
    actionlint
    coreutils
    curl
    gettext
    gnused
    jq
    lsof
    netcat-gnu
    util-linux
    # Git.
    git
    git-lfs
    # Just.
    just
    # Treefmt.
    treefmt
    # Bun.
    bun
    # Node.js.
    #
    # Vitest uses `node:inspector` for coverage which Bun doesn't support yet.
    #
    # https://github.com/oven-sh/bun/issues/4145
    nodejs-slim
    # Python.
    #
    # mkShell effectively requires us to only have 1 Python interpreter per shell.
    #
    # https://github.com/PyO3/maturin/discussions/2176
    # https://github.com/NixOS/nixpkgs/issues/167695
    #
    # Specifically, `EXT_SUFFIX` for all Python interpreters is set to the value for the final one specified.
    #
    # Maturin will emit Python extension files using the virtual environment Python interpreter version's expected extension suffix.
    # Python interpreters with the wrong `EXT_SUFFIX` won't load the Python extension files and throw a ModuleNotFoundError.
    pythonInterpreter
    # Rust.
    rustup
    # uv.
    uv
    # Ruff.
    ruff
    # Pyright.
    pyright
    # Go.
    go_1_26
    # golangci-lint.
    golangci-lint
    # Storage systems.
    multi-storage-client.packages.aistore
    azurite
    fake-gcs-server
    versitygw
    # Telemetry systems.
    grafana
    mimir
    tempo
    # Packaging.
    dpkg
    gnutar
    rpm
    zip
    # OpenBao.
    openbao
    # Teleport.
    teleport
    # AWS CLI.
    awscli2
    # Google Cloud CLI.
    google-cloud-sdk
    # OpenSSL.
    openssl
    # JFrog CLI.
    jfrog-cli
    # OpenSSH.
    openssh
    # GitHub CLI.
    gh
  ];

  shellHook =
    let
      apple-sdk = {
        aarch64 = multi-storage-client.pkgsNative.aarch64-darwin.apple-sdk_14;
      };
    in
    ''
      # Dynamic linker.
      #
      # https://discourse.nixos.org/t/how-to-solve-libstdc-not-found-in-shell-nix/25458
      # https://discourse.nixos.org/t/poetry-pandas-issue-libz-so-1-not-found/17167
      export LD_LIBRARY_PATH=${
        lib.strings.makeLibraryPath [
          stdenv.cc.cc.lib
          zlib
        ]
      }

      # Apple SDKs.
      export APPLE_SDK_AARCH64=${apple-sdk.aarch64.sdkroot}
      export APPLE_SDK_VERSION_AARCH64=${apple-sdk.aarch64.version}

      # Disable Objective-C fork safety on macOS for pytest-xdist.
      #
      # https://github.com/python/cpython/issues/77906
      export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

      # OpenBao/Vault.
      #
      # https://openbao.org/docs/commands#environment-variables
      export VAULT_ADDR=https://prod.vault.nvidia.com
      export VAULT_NAMESPACE=ngc-multi-storage-client

      echo "⚗️"
    '';
}
