#!/bin/sh

[ $1 -eq 0 ] || exit 0

dd if=$RANDOM bs=512 of=outfile
diff -q $RANDOM outfile
