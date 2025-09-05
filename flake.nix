#
# Nix flake.
#
# https://nix.dev/manual/nix/latest/command-ref/new-cli/nix3-flake#flake-format
# https://wiki.nixos.org/wiki/Flakes#Flake_schema
#
{
  description = "Nix flake.";

  inputs = {
    # https://nixos.org/manual/nixpkgs/unstable
    # https://search.nixos.org/packages?channel=unstable
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      ref = "refs/heads/nixos-unstable";
    };
  };

  outputs =
    inputs:
    let
      # Output systems.
      #
      # https://github.com/NixOS/nixpkgs/blob/nixos-unstable/lib/systems/flake-systems.nix
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];

      # Return an attribute set of system to the result of applying `f`.
      #
      # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.genAttrs
      genSystemAttrs = f: inputs.nixpkgs.lib.attrsets.genAttrs systems f;
    in
    {
      # Development shells.
      #
      # For `nix develop` and direnv's `use flake`.
      devShells = genSystemAttrs (
        system:
        let
          # Attribute set of development shell name to Python package.
          devShellPythonPackages = with inputs.nixpkgs.legacyPackages.${system}; {
            default = python310;
            "python3.10" = python310;
            "python3.11" = python311;
            "python3.12" = python312;
            "python3.13" = python313;
          };

          # Create a development shell for a given Python package.
          #
          # https://nixos.org/manual/nixpkgs/unstable#sec-pkgs-mkShell
          #
          # Maturin effectively requires us to only have 1 Python package per shell.
          #
          # https://github.com/PyO3/maturin/issues/2198
          mkDevShell =
            pythonPackage:
            inputs.nixpkgs.legacyPackages.${system}.mkShell {
              packages = with inputs.nixpkgs.legacyPackages.${system}; [
                # Nix.
                #
                # Nix is dynamically linked on some systems. If we set LD_LIBRARY_PATH,
                # running Nix commands with the system-installed Nix may fail due to mismatched library versions.
                nix
                nixfmt-rfc-style
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
                # Python.
                pythonPackage
                # Rust.
                rustup
                # uv.
                uv
                # Ruff.
                ruff
                # Pyright.
                pyright
                # Storage systems.
                azurite
                fake-gcs-server
                minio
                # Telemetry systems.
                grafana
                mimir
                tempo
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
                    aarch64 = inputs.nixpkgs.legacyPackages.aarch64-darwin.apple-sdk_11;
                    x86_64 = inputs.nixpkgs.legacyPackages.x86_64-darwin.apple-sdk_11;
                  };
                in
                ''
                  # Dynamic linker.
                  #
                  # https://discourse.nixos.org/t/how-to-solve-libstdc-not-found-in-shell-nix/25458
                  # https://discourse.nixos.org/t/poetry-pandas-issue-libz-so-1-not-found/17167
                  export LD_LIBRARY_PATH=${
                    with inputs.nixpkgs.legacyPackages.${system};
                    lib.strings.makeLibraryPath [
                      stdenv.cc.cc.lib
                      zlib
                    ]
                  }

                  # Apple SDKs.
                  export APPLE_SDK_AARCH64=${apple-sdk.aarch64}/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
                  export APPLE_SDK_X86_64=${apple-sdk.x86_64}/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
                  export APPLE_SDK_VERSION_AARCH64=${apple-sdk.aarch64.version}
                  export APPLE_SDK_VERSION_X86_64=${apple-sdk.x86_64.version}

                  # Disable Objective-C fork safety on macOS for pytest-xdist.
                  #
                  # https://github.com/python/cpython/issues/77906
                  export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

                  echo "⚗️"
                '';
            };
        in
        inputs.nixpkgs.lib.attrsets.mapAttrs (
          _: pythonPackage: mkDevShell pythonPackage
        ) devShellPythonPackages
      );
    };
}
