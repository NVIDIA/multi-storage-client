{
  buildGoModule,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
buildGoModule (finalAttrs: {
  pname = "aistore";
  version = "1.4.5";

  src = fetchFromGitHub {
    owner = "NVIDIA";
    repo = "aistore";
    tag = "v${finalAttrs.version}";
    hash = "sha256-tHznq3QP/XX3Czh3JwFI0XmiIYo1xYwvZy+c1nTxBL8=";
  };

  vendorHash = "sha256-sjO3Ktrb5H2efYv1C1O597C6XHw9Pzc692XC6oBzH6I=";

  # Exclude `cmd/cli` and `cmd/ishard` which are separate Go modules.
  #
  # https://github.com/NVIDIA/aistore/tree/v1.4.5/cmd
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
  # https://github.com/NVIDIA/aistore/blob/v1.4.5/Makefile#L86
  ldflags = [
    "-X main.build=v${finalAttrs.version}"
    "-X main.buildtime=1970-01-01T00:00:00-00:00"
  ];

  tags = [
    # Monotonic time.
    #
    # https://github.com/NVIDIA/aistore/blob/v1.4.5/Makefile#L98
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
