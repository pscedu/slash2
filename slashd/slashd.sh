#!/usr/bin/env bash
# $Id$

prog=slashd
ctl=slmctl

. $(dirname $0)/pfl_daemon.sh

usage()
{
	echo "usage: $0 [-F filter] [-P profile] [-gv]" >&2
	exit 1
}

bkav=("$@")
while getopts "F:gP:v" c; do
	case $c in
	F) filter=$OPTARG	;;
	g) filter=mygdb		;;
	P) prof=$OPTARG		;;
	v) verbose=1		;;
	*) usage		;;
	esac
done
shift $(($OPTIND - 1))

apply_host_prefs "$@"

base=$dir/$prof.s2
# Initialization/configuration
ulimit -n 100000
ulimit -c $((1024 * 1024 * 1024 * 50))
sysctl $( [ $verbose -eq 0 ] && printf %s -q ) -w vm.max_map_count=500000
export LD_LIBRARY_PATH=/usr/local/lib
export PSC_SYSLOG=1
export PSC_LOG_LEVEL=${PSC_LOG_LEVEL:-notice}
export PSC_LOG_LEVEL_info=info
export PSC_LOG_FILE=$base/log/$host.$name/%t
export PSC_LOG_FILE_LINK=$base/log/$host.$name/latest
export CONFIG_FILE=$base/slcfg

type modprobe >/dev/null 2>&1 && modprobe fuse

preproc
$filter $prog -D $base/var
postproc $?
vsleep $start
exec $0 "${bkav[@]}"
