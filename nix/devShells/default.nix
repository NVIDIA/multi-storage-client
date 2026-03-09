{
  azurite,
  bun,
  coreutils,
  curl,
  dpkg,
  fake-gcs-server,
  gh,
  git,
  git-lfs,
  gnutar,
  go_1_26,
  golangci-lint,
  grafana,
  jfrog-cli,
  just,
  lib,
  lsof,
  mimir,
  mkShell,
  multi-storage-client,
  netcat-gnu,
  nix,
  nixfmt,
  openssh,
  pyright,
  python310,
  pythonInterpreter ? python310,
  rpm,
  ruff,
  rustup,
  stdenv,
  tempo,
  treefmt,
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
    coreutils
    curl
    lsof
    netcat-gnu
    # Git.
    git
    git-lfs
    # Just.
    just
    # Treefmt.
    treefmt
    # Bun.
    bun
    # Python.
    #
    # Maturin effectively requires us to only have 1 Python package per shell.
    #
    # https://github.com/PyO3/maturin/issues/2198
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
        aarch64 = multi-storage-client.nativePackages.aarch64-darwin.apple-sdk_14;
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

      echo "⚗️"
    '';
}
