dep wget md5deep

wget https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.3.tar.xz
tar xf linux-4.3.tar.xz

dir=$(pwd)
(
	cd $LOCAL_TMP
	tar xf $dir/linux-4.3.tar.xz
)
md5deep $LOCAL_TMP/linux-4.3 linux-4.3
diff -qr $LOCAL_TMP/linux-4.3 linux-4.3

make oldconfig </dev/null
make -j$(nproc) bzImage
