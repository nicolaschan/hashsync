{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.rustup
  ];

  shellHook = ''
    # Setting up the Rust toolchain
    rustup default stable
    rustup update
  '';
}

