#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep git

git clone ssh://source/a proj
cd proj
./bootstrap.sh

make build
make test

# XXX it would be valuable to perform a build on a local file system and
# compare binaries between there and here but debugging symbols probably
# contain filenames...

cd ..

# XXX this should use a local file system
cp -R proj proj.bak
diff -qr proj proj.bak
