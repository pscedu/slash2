#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=4.3
dst=$(pwd)/tarssh

wget -nv https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-$V.tar.xz
decompress_xz linux-$V.tar.xz | tar fx -
mkdir $dst
tar fc - linux-$V | ssh localhost "tar fCx - $dst"
diff -r linux-$V $dst/linux-$V
