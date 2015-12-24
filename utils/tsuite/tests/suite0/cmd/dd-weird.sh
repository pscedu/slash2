#!/bin/sh

set -e

[ $1 -eq 0 ] || exit 0

dd if=$RANDOM of=r000 seek=1M count=1 bs=64k
md5sum r000

dd if=$RANDOM of=r000 seek=1G count=1 bs=64k
dd if=$RANDOM of=r000 seek=1M count=1 bs=64k
