#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=2.84

exclude_time_start
wget -nv http://download.transmissionbt.com/files/transmission-$V.tar.xz
exclude_time_end

cd transmission-$V
./configure enable-cli
make