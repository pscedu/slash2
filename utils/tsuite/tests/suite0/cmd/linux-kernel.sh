#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=4.4.4

tsuite_wget 87307804 73d1835cfb6dd348d87c8c2413190c21 \
    4d39d79f5889ea60365269387ce21520ba767c15837a136c12c3f5ca2b48812c \
    https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-$V.tar.xz

tsuite_decompress linux-$V.tar.xz | tar fx -

dir=$(pwd)
(
	# XXX the local file system is subject to timing variation we do
	# not want to include in the test; better to use tmpfs.
	cd $LOCAL_TMP
	tsuite_decompress $dir/linux-$V.tar.xz | tar fx -
)
diff -qr $LOCAL_TMP/linux-$V linux-$V

cd linux-$V
make oldconfig </dev/null
make bzImage
make modules
