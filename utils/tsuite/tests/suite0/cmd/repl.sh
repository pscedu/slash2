#!/bin/sh

[ $1 -eq 0 ] || exit 0

repl_wait()
{
	(
	set +x

	ios=$1
	fn=$2
	cnt=0
	last=
	timeout=500

	while :; do
		new=$(msctl -H repl-status $fn)$'\n'
		if [ x"$last" = x"$new" ]; then
			[ $(( cnt++ )) -eq $timeout ] && \
			    die "no change in $timeout seconds; something is broken"
		else
			diff -u <(echo -n "$last") <(echo -n "$new") || :
			cnt=0
		fi
		echo $new | grep -A1 $ios | tail -1 | grep -q '[qs]' || break
		sleep 1
		last=$new
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
