#!/bin/sh
# $Id$

set -e

host=$1
prog=$2
sock=$3
flags=$4
cnt=0

mkdir ctl/log.$prog.$host

while :; do
	$prog -S $sock $flags | tee ctl/log.$prog.$host/$(printf "%06d" $cnt)
	sleep 1
	cnt=$(($cnt + 1))
	clear
done
