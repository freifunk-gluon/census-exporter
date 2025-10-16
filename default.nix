with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "impurePythonEnv";

  buildInputs = with python3Packages; [
    python3Full
    black
    click
    colorama
    mypy
    prometheus_client
    requests
    ruff
    structlog
    types-requests
    voluptuous
  ];
}
