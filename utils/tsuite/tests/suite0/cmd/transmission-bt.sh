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

exit 0

torrent_fn=random_data.torrent

if [ $1 -eq 0 ]; then
	# Hosting client
	./utils/transmission-create -o $torrent_fn $RANDOM_DATA
	./utils/transmission-daemon -c .
else
	# Peers that will download the file.

	# Wait for the .torrent file to appear.
	until [ -e $torrent_fn ]; do
		sleep 1
	done

	./utils/transmission-cli random_data.torrent
fi
