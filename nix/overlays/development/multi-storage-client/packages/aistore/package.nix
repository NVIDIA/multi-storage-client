{
  buildGoModule,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
buildGoModule (finalAttrs: {
  pname = "aistore";
  version = "1.4.4";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "aistore";
    tag = "v${finalAttrs.version}";
    hash = "sha256-GAu+KKm5BeDBSpX4QY4CWdIMqIU+ESqvv0EGUGjwlWM=";
  };

  vendorHash = "sha256-ESXiEBtGtsQApoVPmmLMkDR17dKJzMcWxG49n5l9IjA=";

  # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
  #
  # https://github.com/NVIDIA/aistore/tree/v1.4.4/cmd
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
  # https://github.com/NVIDIA/aistore/blob/v1.4.4/Makefile#L86
  ldflags = [
    "-X main.build=v${finalAttrs.version}"
    "-X main.buildtime=1970-01-01T00:00:00-00:00"
  ];

  tags = [
    # Monotonic time.
    #
    # https://github.com/NVIDIA/aistore/blob/v1.4.4/Makefile#L98
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
