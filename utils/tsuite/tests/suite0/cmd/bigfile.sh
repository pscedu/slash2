#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep sft

niters=100
sizes=$(cat <<EOF)
	123
	24789
	770924789
	524789
	22524789
	57824789
	52478900
	2111524789
	3111520000
EOF

for i in $(seq $niters); do
	for j in $(seq $sizes); do
		# $RANDOM gives you a 'random' value between [0,32767]
		# so we scale that out between [1k,128k]
		bs=$((127*1024 * RANDOM/32767 + 1024))
		fn=t.$i.$j
		sft -w -s $size -b $bs $LOCAL_TMP/$fn
		dd if=$LOCAL_TMP/$fn of=$fn bs=$bs
		diff -q $LOCAL_TMP/$fn $fn
	done
done
