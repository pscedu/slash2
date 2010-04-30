#!/bin/sh
# $Id$

usage()
{
	echo "usage: $0 hostname ctlcmd ..." >&2
	exit 1
}

set -e

host=$1
shift

prog=$2
name=${prog##*/}
shift

flags="$@"

dir=ctl/log.$name.$host
mkdir $dir

cnt=0
while $prog $flags | tee $dir/$(printf "%06d" $cnt); do
	sleep 1
	cnt=$(($cnt + 1))
	clear
done
