#!/bin/sh

# @result int ^\s*Directory creates per second\s+=\s(\d+)
# @result int ^\s*File creates per second\s+=\s(\d+)
# @result int ^\s*File removals per second\s+=\s(\d+)
# @result int ^\s*Directory removals per second\s+=\s+(\d+)

[ $1 -eq 0 ] || exit 0

$SRC/slash2/tests/fdtree.bash -f 3 -d5
