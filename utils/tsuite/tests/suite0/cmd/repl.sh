#!/bin/sh

[ $1 -eq 0 ] || exit 0

repl_wait()
{
	(
	set +x

	ios=$1
	fn=$2
	cnt=0

	while msctl -H repl-status $fn | \
	    grep -A1 $ios | tail -1 | grep -q q; do
		[ $(( cnt++ % 60 )) -eq 0 ] && msctl repl-status $fn
		sleep 1
	done
	)
}

fn=t000

dd if=$RANDOM_DATA of=$fn bs=131072

cksum=$(md5sum $fn)

msctl repl-add:io0@SITE0,io1@SITE0:* $fn

repl_wait io0@SITE0 $fn
repl_wait io1@SITE0 $fn

msctl repl-remove:io0@SITE0:* $fn

echo $cksum | md5sum -c

msctl repl-add:io0@SITE0:* $fn
repl_wait io0@SITE0 $fn
msctl repl-remove:io1@SITE0:* $fn

echo $cksum | md5sum -c
