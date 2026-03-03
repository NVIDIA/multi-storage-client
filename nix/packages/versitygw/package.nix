{
  buildGoModule,
  fetchFromGitHub,
  lib,
  versionCheckHook,
}:
# https://nixos.org/manual/nixpkgs/unstable#ssec-language-go
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

  nativeInstallCheckInputs = [
    versionCheckHook
  ];

  versionCheckProgram = "${builtins.placeholder "out"}/bin/versitygw";

  versionCheckProgramArg = "--version";

  meta = {
    description = "High-performance S3 translation service";
    homepage = "https://github.com/versity/versitygw";
    license = lib.licenses.asl20;
    mainProgram = "versitygw";
  };
})
