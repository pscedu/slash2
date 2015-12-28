#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep rsync

rsync -B 131072 $RANDOM_DATA r000
rsync -B 131072 r000 localhost:$(pwd)/r001
diff -q r000 r001
