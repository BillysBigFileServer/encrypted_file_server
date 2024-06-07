{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-overlay.follows = "rust-overlay";
    };

  };

  outputs = { self, nixpkgs, crane, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        rustToolchain =
          pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        craneLib = crane.lib.${system}.overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource (craneLib.path ./.);
        buildInputs = with pkgs;
          [ clang_15 libsodium protobuf openssl pkg-config mold-wrapped ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];
        cargoArtifacts = craneLib.buildDepsOnly {
          inherit src;
          buildInputs = buildInputs;
        };
        cargoArtifactsProd = craneLib.buildDepsOnly {
          inherit src;
          buildInputs = buildInputs;
          cargoExtraArgs = "--features prod";
        };

        my-crate = craneLib.buildPackage {
          inherit cargoArtifacts src;
          cargoVendorDir =
            craneLib.vendorCargoDeps { cargoLock = ./Cargo.lock; };

          buildInputs = buildInputs;
        };

        my-crate-prod = craneLib.buildPackage {
          inherit cargoArtifactsProd src;
          cargoVendorDir =
            craneLib.vendorCargoDeps { cargoLock = ./Cargo.lock; };
          buildInputs = buildInputs;
          cargoExtraArgs = "--features prod";
        };
      in {
        packages.default = my-crate;
        packages.build-prod = my-crate-prod;
        packages.build-deps-prod = cargoArtifactsProd;

        devShells.default = craneLib.devShell {
          # Automatically inherit any build inputs from `my-crate`
          inputsFrom = [ my-crate ];

          TOKEN_PUBLIC_KEY =
            "9a52596722961209b2bbed222136e1e50d705c6c530da06e0442b5c1a5dda457";

          packages = with pkgs; [
            cargo-outdated
            cargo-watch
            protolint
            sqlx-cli
            rust-analyzer
            openssl
            xxd
          ];
        };
      });
}
