#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=5.3.0

exclude_time_start
wget -nv http://mirrors-usa.go-parts.com/gcc/releases/gcc-$V/gcc-$V.tar.bz2
exclude_time_end

decompress_bz2 gcc-$V.tar.bz2 | tar fx -
cd gcc-$V
./configure
make
make test
