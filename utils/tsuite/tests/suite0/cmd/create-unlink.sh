#!/bin/sh

[ $1 -eq 0 ] || exit 0

nfiles=10000

for i in $(seq $nfiles); do
	fn=t$i
	touch $fn
	rm $fn
	ls
done
