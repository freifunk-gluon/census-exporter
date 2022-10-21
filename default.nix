with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "impurePythonEnv";

  buildInputs = with python3Packages; [
    poetry
  ];
}
