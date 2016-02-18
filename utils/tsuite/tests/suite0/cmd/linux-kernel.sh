#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=4.3

exclude_time_start
wget -nv https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-$V.tar.xz
exclude_time_end

decompress_xz linux-$V.tar.xz | tar fx -

dir=$(pwd)
(
	cd $LOCAL_TMP
	decompress_xz $dir/linux-$V.tar.xz | tar fx -
)
diff -qr $LOCAL_TMP/linux-$V linux-$V

cd linux-$V
make oldconfig </dev/null
make bzImage
