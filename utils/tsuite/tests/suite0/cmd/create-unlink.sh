#!/bin/sh

nfiles=10000

for i in $(seq $nfiles); do
	fn=t$i
	touch $fn
	rm $fn
	ls
done
