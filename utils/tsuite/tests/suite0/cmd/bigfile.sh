#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep sft

niters=100
sizes='
	123
	24789
	770924789
	524789
	22524789
	57824789
	52478900
	2111524789
	3111520000
'

for i in $(seq $niters); do
	for size in $sizes; do
		# $RANDOM gives you a 'random' value between [0,32767]
		# so we scale that out between [1k,128k]
		blocksize=$((127*1024 * RANDOM/32767 + 1024))
		basefn=t.$i.$size
		localfn=$LOCAL_TMP/$basefn
		sl2fn=$basefn
		touch $localfn
		sft -w -s $size -b $blocksize $localfn
		dd if=$localfn of=$sl2fn bs=$blocksize
		diff -q $localfn $sl2fn
		rm $localfn $sl2fn
	done
done
