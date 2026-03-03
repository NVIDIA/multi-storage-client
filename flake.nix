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
                inputs.self.overlays.default
              ];
            }
          ) inputs.nixpkgs.legacyPackages;
        };
      };
    in
    {
      # ────────────────────────────────────────────────────────────────────────────────────────────────────
      #
      # System-agnostic outputs.
      #
      # ────────────────────────────────────────────────────────────────────────────────────────────────────

      # Overlays.
      #
      # Include dependency overlays with `inputs.nixpkgs.lib.fixedPoints.composeManyExtensions`.
      #
      # https://nixos.org/manual/nixpkgs/unstable#function-library-lib.fixedPoints.composeManyExtensions
      overlays = {
        default = final: prev: {
          multi-storage-client = {
            # Expose packages for other systems to support cross-compilation in development shells (e.g. macOS SDK).
            nativePackages = finalInputs.nixpkgs.legacyPackages;

            packages = final.lib.filesystem.packagesFromDirectoryRecursive {
              inherit (final) callPackage;
              directory = ./nix/packages;
            };

            devShells = final.lib.filesystem.packagesFromDirectoryRecursive {
              inherit (final) callPackage;
              directory = ./nix/devShells;
            };
          };
        };
      };

      # ────────────────────────────────────────────────────────────────────────────────────────────────────
      #
      # System-specific outputs.
      #
      # ────────────────────────────────────────────────────────────────────────────────────────────────────

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
    };
}
