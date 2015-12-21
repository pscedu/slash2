#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep rsync

# XXX make this actually use sockets
rsync $RANDOM r000
