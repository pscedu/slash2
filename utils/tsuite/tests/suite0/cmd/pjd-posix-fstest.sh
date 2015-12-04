dir=$SRC/distrib/posix-fstest

(
	cd $dir
	make
)

prove -r $dir
