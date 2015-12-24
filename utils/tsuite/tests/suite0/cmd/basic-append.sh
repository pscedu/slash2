#!/bin/sh

set -e

[ $1 -eq 0 ] || exit 0

fn=tf
echo line1 >> $fn
echo line2 >> $fn
echo line3 >> $fn
echo line4 >> $fn
nl=$(wc -l $fn | sed 's/ //g')
[ x"$nl" = x"4" ]

diff -q $fn - <<EOF
line1
line2
line3
line4
EOF
