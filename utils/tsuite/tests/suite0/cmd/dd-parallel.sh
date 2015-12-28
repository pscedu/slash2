#!/bin/sh

[ $1 -eq 0 ] || exit 0

ndirs=100
nfiles_per_dir=100
jobs_max=100

for i in $(seq $ndirs); do
	mkdir d$i
	for j in $(seq $nfiles_per_dir); do
		dd if=$RANDOM_DATA of=d$i/t$j bs=32768 count=128 &

		while :; do
			# sleep for a little to prevent job overspawning
			nj=$(jobs | wc -l)
			[ $nj -le $jobs_max ] && break
			sleep 1
		done
	done
done
wait
