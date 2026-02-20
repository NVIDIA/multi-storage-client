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
      # Packages.
      #
      # For `nix build`.
      packages = genSystemAttrs (system: {
        aistore =
          with inputs.nixpkgs.legacyPackages.${system};
          buildGoModule (finalAttrs: {
            pname = "aistore";
            version = "1.4.2";

            src = fetchFromGitHub {
              owner = "NVIDIA";
              repo = "aistore";
              tag = "v${finalAttrs.version}";
              hash = "sha256-W8T58Tj7AODJknMuDNnQrPFECxSit5nbRJLoGDl1j9s=";
            };

            vendorHash = "sha256-blBJNIauFjgp8KGzsTTAo7KETsRh0lee0jZJuoVgxvw=";

            # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
            #
            # https://github.com/NVIDIA/aistore/tree/v1.4.2/cmd
            subPackages = [
              "cmd/aisinit"
              "cmd/aisloader"
              "cmd/aisnode"
              "cmd/aisnodeprofile"
              "cmd/authn"
              "cmd/xmeta"
            ];

            # Needed for version strings.
            #
            # https://github.com/NVIDIA/aistore/blob/v1.4.2/Makefile#L86
            ldflags = [
              "-X main.build=v${finalAttrs.version}"
              "-X main.buildtime=1970-01-01T00:00:00-00:00"
            ];

            tags = [
              # Monotonic time.
              #
              # https://github.com/NVIDIA/aistore/blob/v1.4.2/Makefile#L98
              "mono"
            ];

            doInstallCheck = true;

            nativeInstallCheckInputs = [
              versionCheckHook
            ];

            versionCheckProgram = "${builtins.placeholder "out"}/bin/aisnode";

            versionCheckProgramArg = "-h";

            meta = {
              description = "Scalable storage for AI applications";
              homepage = "https://github.com/NVIDIA/aistore";
              license = lib.licenses.mit;
              mainProgram = "aisnode";
            };
          });

        versitygw =
          with inputs.nixpkgs.legacyPackages.${system};
          buildGoModule (finalAttrs: {
            pname = "versitygw";
            version = "1.3.0-dev";

            src = fetchFromGitHub {
              owner = "versity";
              repo = "versitygw";
              # 1.2.0 has an ETag equality bug that breaks conditional reads/writes.
              #
              # https://github.com/versity/versitygw/issues/1835
              rev = "3856f999048ec174f6473a214b50cd21ba6bb27b";
              hash = "sha256-8yjC9d5BKx5Q/RMtQL59t4mavFohC7+7yMr/0i34vG0=";
            };

            vendorHash = "sha256-z+m5ez17yF+GcUHyKU6a3Q69A6ACBVk0gCjKIaIJ554=";

            # Requires access to S3.
            doCheck = false;

            # Needed for "versitygw --version" to not show placeholders.
            ldflags = [
              "-X main.Build=v${finalAttrs.version}"
              "-X main.BuildTime=1980-01-01T00:00:02Z"
              "-X main.Version=v${finalAttrs.version}"
            ];

            doInstallCheck = true;

            nativeInstallCheckInputs = [ versionCheckHook ];

            versionCheckProgram = "${builtins.placeholder "out"}/bin/versitygw";

            versionCheckProgramArg = "--version";

            meta = {
              description = "High-performance S3 translation service";
              homepage = "https://github.com/versity/versitygw";
              license = lib.licenses.asl20;
              mainProgram = "versitygw";
            };
          });
      });

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
                # Bun.
                bun
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
                # Go.
                go_1_26
                # golangci-lint.
                golangci-lint
                # Storage systems.
                inputs.self.packages.${system}.aistore
                azurite
                fake-gcs-server
                inputs.self.packages.${system}.versitygw
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
                    aarch64 = inputs.nixpkgs.legacyPackages.aarch64-darwin.apple-sdk_14;
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
                  export APPLE_SDK_VERSION_AARCH64=${apple-sdk.aarch64.version}

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
