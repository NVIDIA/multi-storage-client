{
  buildGoModule,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
buildGoModule (finalAttrs: {
  pname = "aistore";
  version = "1.5.0";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "aistore";
    tag = "v${finalAttrs.version}";
    hash = "sha256-u2hJr+Y5Dd7MRxJVGd4kz8BgfSQmhyyi4lCUZhNTuC4=";
  };

  vendorHash = "sha256-jKqrmXSGblzWZUZT9olHkZSSVU6aydxFu3JtYfiSbYY=";

  # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
  #
  # https://github.com/NVIDIA/aistore/tree/v1.5.0/cmd
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
  # https://github.com/NVIDIA/aistore/blob/v1.5.0/Makefile#L87
  ldflags = [
    "-X main.build=v${finalAttrs.version}"
    "-X main.buildtime=1970-01-01T00:00:00-00:00"
  ];

  tags = [
    # Monotonic time.
    #
    # https://github.com/NVIDIA/aistore/blob/v1.5.0/Makefile#L99
    "mono"
  ];

  __darwinAllowLocalNetworking = true;

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
