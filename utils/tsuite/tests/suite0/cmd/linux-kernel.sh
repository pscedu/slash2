#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=4.3

wget -nv https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-$V.tar.xz
tar xf linux-$V.tar.xz

dir=$(pwd)
(
	cd $LOCAL_TMP
	tar xf $dir/linux-$V.tar.xz
)
diff -qr $LOCAL_TMP/linux-$V linux-$V

cd linux-$V
make oldconfig </dev/null
make -j$(nproc) bzImage
