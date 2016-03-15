#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep iozone

iozone -u 4 -l 1 -i 0 -i 1 -r 256 -s 10240000 -b result -e
