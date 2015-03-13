#!/usr/bin/env bash
# $Id$
#
# Build SLASH2 source code in a loop.

cleanup()
{
	echo

	end_time=$SECONDS

	ss=`expr $end_time - $start_time`

	mm=`expr $ss / 60`
	ss=`expr $ss % 60`
	hh=`expr $mm / 60`
	mm=`expr $mm % 60`

	printf "\nTotal time elapsed %02d::%02d::%02d\n" $hh $mm $ss
	exit 0
}

usage()
{
	echo "usage: $0 change dir count" >&2
	exit 1
}

output=/dev/null
while getopts "v" c; do
	case $c in
		v) output=/dev/stdout;;
		*) usage;;
	esac
done
shift $((OPTIND - 1))

[ $# -eq 3 ] || usage

# trap Control+C

trap 'cleanup' 2

start_time=$SECONDS

set -e

mydir=`pwd`
for ((i=0; i < $3; i++)); do
	dir=$2.$i

	mkdir $dir
	svn co -r $1 svn+ssh://frodo/cluster/svn/projects $dir
	cd $dir
	make up
	OBJBASE=$(pwd)/obj make >$output
	cd $mydir
done

cleanup
