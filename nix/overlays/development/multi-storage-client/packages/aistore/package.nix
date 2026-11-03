{
  buildGo127Module,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
buildGo127Module (finalAttrs: {
  pname = "aistore";
  version = "1.5.2";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "aistore";
    tag = "v${finalAttrs.version}";
    hash = "sha256-nkC4+8sXoGG7hSKUJFd2GVjcBo10P65Up4X7sVt89Yw=";
  };

  vendorHash = "sha256-vd4zA7iHEZ5+3OukBuB9Dg1+VBxqfGxN8NPwKttoBIs=";

  # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
  #
  # https://github.com/NVIDIA/aistore/tree/v1.5.2/cmd
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
  # https://github.com/NVIDIA/aistore/blob/v1.5.2/Makefile#L90
  ldflags = [
    "-X main.build=v${finalAttrs.version}"
    "-X main.buildtime=1970-01-01T00:00:00-00:00"
  ];

  tags = [
    # Monotonic time.
    #
    # https://github.com/NVIDIA/aistore/blob/v1.5.2/Makefile#L102
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
