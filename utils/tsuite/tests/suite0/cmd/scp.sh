#!/bin/sh

[ $1 -eq 0 ] || exit 0

scp $RANDOM_DATA r000
scp r000 localhost:$(pwd)/r001
diff -q r000 r001
