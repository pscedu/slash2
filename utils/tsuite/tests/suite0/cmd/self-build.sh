@dep git md5deep

git clone ssh://source/a proj
cd proj
./bootstrap.sh

make build
make test

# XXX it would be valuable to perform a build in the local and compare
# binaries but debugging symbols probably contain filenames

cd ..

cp -R proj $LOCAL_TMP/proj
md5deep proj $LOCAL_TMP/proj
diff -qr proj $LOCAL_TMP/proj
