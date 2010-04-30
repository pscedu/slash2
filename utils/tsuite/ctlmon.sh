#!/bin/sh
# $Id$

usage()
{
	echo "usage: $0 hostname ctlcmd ..." >&2
	exit 1
}

if [ $# -lt 3 ]; then
	usage
fi

set -e

host=$1
shift

prog=$1
name=${prog##*/}
shift

flags="$@"

dir=ctl/log.$name.$host
mkdir -p $dir

cnt=0
while $prog $flags | tee $dir/$(printf "%06d" $cnt); do
	sleep 1
	cnt=$(($cnt + 1))
	clear
done
