#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=2.84

tsuite_wget 3077836 411aec1c418c14f6765710d89743ae42 \
    a9fc1936b4ee414acc732ada04e84339d6755cd0d097bcbd11ba2cfc540db9eb \
    http://download.transmissionbt.com/files/transmission-$V.tar.xz

tsuite_decompress transmission-$V.tar.xz | tar fx -

cd transmission-$V
./configure --enable-cli
make

exit 0

min_sysctl net.core.rmem_max=4194304
min_sysctl net.core.wmem_max=1048576

torrent_fn=random_data.torrent

if [ $1 -eq 0 ]; then
	# Hosting client: launch tracker.
	(
		cd udpt
		make
	)
	(
		cd udpt
		pkill udpt || :
		./udpt
	) &

	# Create torrent.
	port=9091
	./utils/transmission-create -o $torrent_fn -t udp://$(hostname):$port $RANDOM_DATA

	# Seed.
	pkill transmission-daemon || :
	./utils/transmission-daemon -c .

	# Wait for peers to finish.
	shopt -s nullglob
	while :; do
		finished=(me ../finished-*)
		[ ${#finished[@]} -eq $2 ] && break
		sleep 1
	done

	# Cleanup.
	pkill transmission-daemon
	pkill udpt
	wait
else
	# Peers that will download the file.  Wait for the torrent to
	# appear, download it, then exit.
	until [ -e $torrent_fn ]; do
		sleep 1
	done

	dir=dl-$1
	mkdir -p $dir
	./utils/transmission-cli -w $dir $torrent_fn

	touch ../finished-$1
fi
