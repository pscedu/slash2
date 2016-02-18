#!/bin/sh

[ $1 -eq 0 ] || exit 0

dir=$SRC/distrib/posix-fstest

(
	cd $dir
	make
)

shopt -s extglob

# SLASH2 currently only supports
#
#    strlen(name) + strlen(dst) <= SL_TWO_NAME_MAX (364)
#
# for journaling reasons, so disable the long dst name test.
sed -i '13,14s/^/#/' $dir/tests/symlink/03.t
sed -i '9s/14/12/' $dir/tests/symlink/03.t

sudo prove -r $dir/tests/!(truncate)/
