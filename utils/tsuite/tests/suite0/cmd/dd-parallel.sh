#!/bin/sh
# $Id$

ndirs=100
nfiles_per_dir=100
jobs_max=100

for i in $(seq $ndirs); do
	mkdir d$i
	for j in $(seq $nfiles_per_dir); do
		dd if=$RANDOM of=d$i/t$j bs=32768 count=128 &

		while :; do
			# sleep for a little to prevent job overspawning
			nj=(jobs | wc -l)
			[ $nj -gt $njobs_max ] && break
			sleep 1;
		done
	done
done
wait
