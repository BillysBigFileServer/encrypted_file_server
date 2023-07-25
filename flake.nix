{
  inputs = {
    naersk.url = "github:nmattia/naersk/master";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils, naersk, ... }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        naersk-lib = pkgs.callPackage naersk { };
        # ...
      in
      {
        defaultPackage = naersk-lib.buildPackage {
          src = ./.;
          doCheck = false;
          pname = "shooter2_server";
          nativeBuildInputs = [ pkgs.makeWrapper ];
        };

        defaultApp = utils.lib.mkApp {
          drv = self.defaultPackage."${system}";
        };

        devShell = with pkgs; mkShell {
          buildInputs = [
            curl
          
            cargo
            rust-analyzer
            rustPackages.clippy
            rustc
            rustfmt
            sqlx-cli
            sqlite
          ];
        };
      });
}
