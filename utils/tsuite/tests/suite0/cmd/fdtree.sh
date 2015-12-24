#!/bin/sh

set -e

[ $1 -eq 0 ] || exit 0

$SRC/slash2/tests/fdtree.bash -f 3 -d5
