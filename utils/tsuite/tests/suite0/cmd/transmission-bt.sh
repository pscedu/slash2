#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=2.84

exclude_time_start
wget -nv http://download.transmissionbt.com/files/transmission-$V.tar.xz
exclude_time_end

decompress_xz transmission-$V.tar.xz | tar fx -

cd transmission-$V
./configure enable-cli
make

min_sysctl net.core.rmem_max=4194304
min_sysctl net.core.wmem_max=1048576

exit 0

torrent_fn=random_data.torrent

if [ $1 -eq 0 ]; then
	# Hosting client: launch tracker.
	(
		exclude_time_start
		exclude_time_end
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
