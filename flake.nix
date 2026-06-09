{
  description = "k9rs - A fast Kubernetes TUI, inspired by k9s, built in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      # Home Manager module (`programs.k9rs`). System-independent, so it lives
      # outside `eachDefaultSystem`. `self` is threaded in so the module can
      # default its package to this flake's build.
      hmModule = import ./nix/hm-module.nix self;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = rec {
          k9rs = pkgs.rustPlatform.buildRustPackage {
            pname = "k9rs";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;

            meta = with pkgs.lib; {
              description = "A fast Kubernetes TUI, inspired by k9s, built in Rust";
              homepage = "https://github.com/rt-kill/k9rs";
              license = licenses.mit;
              mainProgram = "k9rs";
            };
          };
          default = k9rs;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.k9rs ];
          packages = with pkgs; [
            cargo
            rustc
            rust-analyzer
            clippy
            rustfmt
          ];
        };
      }
    ) // {
      homeModules.default = hmModule;
      homeModules.k9rs = hmModule;
    };
}
