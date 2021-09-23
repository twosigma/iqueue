with import <nixpkgs> {};

stdenv.mkDerivation rec {
  name = "iqueue";
  src = ./.;
  buildInputs = [ microsoft_gsl autoconf automake libtool pkgconfig libbsd autoreconfHook ];
}
