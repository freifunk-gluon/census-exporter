with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "impurePythonEnv";

  buildInputs = with python3Packages; [
    python3Full
    black
    click
    colorama
    prometheus_client
    requests
    ruff
    structlog
    voluptuous
  ];
}
