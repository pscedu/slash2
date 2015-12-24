#!/bin/sh

set -e

[ $1 -eq 0 ] || exit 0

# XXX make this actually use sockets
# scp $RANDOM localhost:r000
# scp localhost:$RANDOM r000
scp $RANDOM r000
