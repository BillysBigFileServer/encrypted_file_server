{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        craneLib = crane.lib.${system};
        my-crate = craneLib.buildPackage {
          src = craneLib.cleanCargoSource (craneLib.path ./.);

          buildInputs = with pkgs; [
              clang_15
              mold
              sqlite
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.libiconv
          ];

        };
      in
      {
        packages.default = my-crate;

        devShells.default = craneLib.devShell {
          # Automatically inherit any build inputs from `my-crate`
          inputsFrom = [ my-crate ];

          packages = with pkgs; [
            cargo-outdated
            cargo-watch
            sqlx-cli
          ];
        };
      });
}
