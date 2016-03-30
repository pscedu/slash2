#!/bin/sh

[ $1 -eq 0 ] || exit 0

repl_wait()
{
	ios=$1
	fn=$2

	while msctl -H repl-status $fn | \
	    grep -A1 $ios | tail -1 | grep q; do
		sleep 1
	done
}

fn=t000

dd if=$RANDOM_DATA of=$fn bs=131072

msctl repl-add:io0@SITE0,io1@@SITE0:* $fn

msctl repl-status $fn

repl_wait io0@SITE0 $fn
repl_wait io1@SITE0 $fn

msctl repl-status $fn
