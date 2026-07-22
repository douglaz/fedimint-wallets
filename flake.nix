{
  description = "fedimint-wallets: walletd (24/7 fedimint wallet daemon) + wallet-cli";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    flakebox = {
      # Same rev fedimint pins — we build against their SDK, so we build with their tooling.
      url = "github:dpc/flakebox?rev=34701639bceb5b12e81e2fff913797c0891c919d";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      flakebox,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        flakeboxLib = flakebox.lib.mkLib pkgs {
          config = {
            # This repo predates the flake; don't let flakebox scaffold CI/git-hook files.
            motd.enable = false;
            github.ci.enable = false;
            just.enable = false;
            # flakebox unconditionally passes -Wl,--compress-debug-sections=zstd on
            # Linux, and of its linker choices only mold actually supports it here: the
            # default wild at this rev rejects the flag and nixpkgs binutils ld is built
            # without zstd. (fedimint solves this with a newer wild via an overlay.)
            linker.wild.enable = false;
            linker.mold.enable = true;
          };
        };

        rustSrc = flakeboxLib.filterSubPaths {
          root = builtins.path {
            name = "fedimint-wallets";
            path = ./.;
          };
          paths = [
            "Cargo.toml"
            "Cargo.lock"
            "wallet-core"
            "wallet-api"
            "wallet-fedimint"
            "wallet-cli"
            "wallet-daemon"
          ];
        };

        buildOutputs = (flakeboxLib.craneMultiBuild { }) (
          craneLib':
          let
            craneLib = craneLib'.overrideArgs {
              pname = "fedimint-wallets";
              version = "0.1.0";
              src = rustSrc;
              nativeBuildInputs = [
                pkgs.pkg-config
                pkgs.cmake # aws-lc-sys
                pkgs.rust-bindgen # librocksdb-sys
              ];
            };
          in
          rec {
            workspaceDeps = craneLib.buildWorkspaceDepsOnly { };
            workspaceBuild = craneLib.buildWorkspace { cargoArtifacts = workspaceDeps; };
            walletd = craneLib.buildPackageGroup {
              pname = "walletd";
              packages = [ "wallet-daemon" ];
              mainProgram = "walletd";
            };
            wallet-cli = craneLib.buildPackageGroup {
              pname = "wallet-cli";
              packages = [ "wallet-cli" ];
              mainProgram = "wallet-cli";
            };
          }
        );

        walletd = buildOutputs.release.walletd;
        wallet-cli = buildOutputs.release.wallet-cli;
      in
      {
        packages = {
          default = walletd;
          inherit walletd wallet-cli;

          # OCI image for the k8s deployment. The image carries the nix closure, so the
          # binaries run as-built — no musl cross, no base-image glibc matching. busybox
          # provides /bin/sh + wget for exec health probes; cacert for HTTPS to guardians.
          walletd-image = pkgs.dockerTools.buildLayeredImage {
            name = "registry.galtland.network/walletd/walletd";
            tag = "latest"; # retag with the git rev at push time
            contents = [
              walletd
              wallet-cli
              pkgs.busybox
              pkgs.cacert
            ];
            config = {
              Entrypoint = [ "${walletd}/bin/walletd" ];
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];
            };
          };
        };

        devShells.default = flakeboxLib.mkDevShell {
          packages = [
            pkgs.pkg-config
            pkgs.cmake
            pkgs.rust-bindgen
            pkgs.jq
          ];
        };
      }
    );
}
