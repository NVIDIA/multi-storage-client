#
# Nix flake.
#
# https://nix.dev/manual/nix/latest/command-ref/new-cli/nix3-flake#flake-format
# https://wiki.nixos.org/wiki/Flakes#Flake_schema
#
{
  inputs = {
    # https://nixos.org/manual/nixpkgs/unstable
    # https://search.nixos.org/packages?channel=unstable
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      ref = "refs/heads/nixos-unstable";
    };

    # Python 3.10 (EOL October 2026) was dropped early for NixOS 26.05.
    #
    # https://github.com/NixOS/nixpkgs/pull/490538
    nixpkgs-python310 = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      # Commit right before drop.
      rev = "91e18d3c384a4c5998e720324425b88f53bbe6e4";
    };
  };

  outputs =
    inputs:
    {
      # Nixpkgs overlays.
      #
      # Include dependency overlays with `inputs.nixpkgs.lib.fixedPoints.composeManyExtensions`.
      #
      # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.fixedPoints.composeManyExtensions
      overlays = {
        # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.fixedPoints.composeManyExtensions
        development = inputs.nixpkgs.lib.fixedPoints.composeManyExtensions [
          (final: prev: {
            # Python 3.10 (EOL October 2026) was dropped early for NixOS 26.05.
            #
            # https://github.com/NixOS/nixpkgs/pull/490538
            inherit (inputs.nixpkgs-python310.legacyPackages.${final.stdenv.hostPlatform.system}) python310;
          })
          (
            final: prev:
            # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.recursiveUpdate
            inputs.nixpkgs.lib.attrsets.recursiveUpdate prev {
              # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.filesystem.packagesFromDirectoryRecursive
              multi-storage-client = {
                # Expose packages for other systems to support cross-compilation in development shells (e.g. macOS SDK).
                pkgsNative = inputs.nixpkgs.legacyPackages;
              };
            }
          )
          (
            final: prev:
            # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.recursiveUpdate
            inputs.nixpkgs.lib.attrsets.recursiveUpdate prev (
              # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.filesystem.packagesFromDirectoryRecursive
              inputs.nixpkgs.lib.filesystem.packagesFromDirectoryRecursive {
                inherit (final) callPackage;
                directory = ./nix/overlays/development;
              }
            )
          )
        ];
      };
    }
    // (
      let
        # Override inputs.
        #
        # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.recursiveUpdate
        finalInputs = inputs.nixpkgs.lib.attrsets.recursiveUpdate inputs {
          nixpkgs = {
            # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.mapAttrs
            legacyPackages = inputs.nixpkgs.lib.attrsets.mapAttrs (
              localSystem: packages:
              # https://github.com/NixOS/nixpkgs/blob/nixos-unstable/pkgs/top-level/default.nix
              import inputs.nixpkgs {
                inherit localSystem;

                overlays = [
                  inputs.self.overlays.development
                ];
              }
            ) inputs.nixpkgs.legacyPackages;
          };
        };

        # Output systems.
        #
        # https://github.com/NixOS/nixpkgs/blob/nixos-unstable/lib/systems/flake-systems.nix
        systems = [
          "aarch64-darwin"
          "aarch64-linux"
          "x86_64-linux"
        ];

        # Return an attribute set of system to the result of applying `f`.
        #
        # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.attrsets.genAttrs
        genSystemAttrs = f: finalInputs.nixpkgs.lib.attrsets.genAttrs systems f;
      in
      {
        # Packages.
        #
        # For `nix build`.
        packages = genSystemAttrs (
          system: finalInputs.nixpkgs.legacyPackages.${system}.multi-storage-client.packages
        );

        # Development shells.
        #
        # For `nix develop` and direnv's `use flake`.
        devShells = genSystemAttrs (
          system: finalInputs.nixpkgs.legacyPackages.${system}.multi-storage-client.devShells
        );
      }
    );
}
