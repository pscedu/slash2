#!/bin/sh

[ $1 -eq 0 ] || exit 0

V=3.0.1

tsuite_wget 88071 71150025e0bb6ea1761150f48b553065 \
    0cbefbcdb02fb13ba364e102f9e7cc2dcf761698533dac25de446a3a3e81390d \
    https://github.com/LLNL/ior/archive/$V.tar.gz

tsuite_decompress $V.tar.gz | tar fx -
cd ior-$V
./bootstrap
./configure --without-mpiio

# XXX need MPI installed
exit 0

make
./src/IOR -w -r -t 1M -b 2g -e -m -k -F -o IOR.output
