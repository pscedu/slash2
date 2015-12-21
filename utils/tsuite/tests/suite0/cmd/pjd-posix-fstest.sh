#!/bin/sh

[ $1 -eq 0 ] || exit 0

dir=$SRC/distrib/posix-fstest

(
	cd $dir
	make
)

prove -r $dir
