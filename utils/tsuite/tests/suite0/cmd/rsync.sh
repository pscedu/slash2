#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep rsync

rsync $RANDOM_DATA r000
rsync r000 localhost:$(pwd)/r001
diff -q r000 r001
