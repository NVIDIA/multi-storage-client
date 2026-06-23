{
  buildGoModule,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
buildGoModule (finalAttrs: {
  pname = "aistore";
  version = "1.4.7";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "aistore";
    tag = "v${finalAttrs.version}";
    hash = "sha256-wYHAN7h4tvRhUITk7Z5CUmf510tHddk8dsOKEBCp33w=";
  };

  vendorHash = "sha256-df7xiuAqbX7XAKVQyX/KWktnK+0ToNw0gJe8vroZe8Q=";

  # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
  #
  # https://github.com/NVIDIA/aistore/tree/v1.4.7/cmd
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
  # https://github.com/NVIDIA/aistore/blob/v1.4.7/Makefile#L86
  ldflags = [
    "-X main.build=v${finalAttrs.version}"
    "-X main.buildtime=1970-01-01T00:00:00-00:00"
  ];

  tags = [
    # Monotonic time.
    #
    # https://github.com/NVIDIA/aistore/blob/v1.4.7/Makefile#L98
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
})
