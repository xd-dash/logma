{
  description = "Logma lifecycle-only service image for shared Fatline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/b25309931cfda5f0b8805f462a29897eeae50168";
    runtime-artifacts = { url = "path:./runtime-artifacts"; flake = false; };
  };

  outputs = { nixpkgs, runtime-artifacts, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      manifest = builtins.fromJSON (builtins.readFile "${runtime-artifacts}/source.json");
      sha = manifest.logma_sha;
      validSha = builtins.match "[0-9a-f]{40}" sha != null;
      runtime = pkgs.runCommand "logma-lifecycle-runtime" {} ''
        test -x ${runtime-artifacts}/bin/logma-lifecycle
        mkdir -p "$out/bin"
        cp ${runtime-artifacts}/bin/logma-lifecycle "$out/bin/logma-lifecycle"
      '';
      image = pkgs.dockerTools.buildLayeredImage {
        name = "fatline-logma-lifecycle";
        tag = sha;
        contents = [ runtime pkgs.cacert ];
        config = {
          Entrypoint = [ "${runtime}/bin/logma-lifecycle" ];
          Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
        };
      };
    in assert validSha; {
      packages.${system} = { inherit runtime image; default = image; };
    };
}
