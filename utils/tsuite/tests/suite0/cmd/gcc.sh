#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=5.1.0

tsuite_wget 32182980 d5525b1127d07d215960e6051c5da35e \
    b7dafdf89cbb0e20333dbf5b5349319ae06e3d1a30bf3515b5488f7e89dca5ad \
    http://gnu.mirrors.pair.com/gnu/gcc/gcc-$V/gcc-$V.tar.bz2

tsuite_decompress gcc-$V.tar.bz2 | tar fx -
cd gcc-$V
./configure --disable-multilib
make
make check
