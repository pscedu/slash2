#!/usr/bin/env bash
# $Id$

prog=mount_wokfs
mod=slash2client.so
ctl=msctl

PATH=$(dirname $0):$PATH
. pfl_daemon.sh

usage()
{
	echo "usage: $0 [-dgOTv] [-F filter] [-P deployment-profile] [instance]" >&2
	exit 1
}

bkav=("$@")
while getopts "dF:gOP:Tv" c; do
	case $c in
	d) nodaemonize=1	;;
	F) filter=$OPTARG	;;
	g) filter=mygdb		;;
	O) once=1		;;
	P) prof=$OPTARG		;;
	T) testmail=1		;;
	v) verbose=1		;;
	*) usage		;;
	esac
done
shift $(($OPTIND - 1))

xargs=()
apply_host_prefs "$@"

base=$dir/$prof.s2

preinit

: ${mp:=/$prof}
umount -l -f $mp 2>/dev/null
[ -d $mp ] || mkdir -p $mp

# Initialization/configuration
ulimit -n 100000
ulimit -c $((1024 * 1024 * 1024 * 50))
export LD_LIBRARY_PATH=/usr/local/lib
export PSC_SYSLOG_info=1
export PSC_LOG_LEVEL=${PSC_LOG_LEVEL:-notice}
export PSC_LOG_LEVEL_info=info
export PSC_LOG_FILE=${PSC_LOG_FILE:-$base/log/$host.$name/%t}
export PSC_LOG_FILE_LINK=$(dirname $PSC_LOG_FILE)/latest

type modprobe >/dev/null 2>&1 && modprobe fuse

xargs+=(datadir=$base/var)
xargs+=(slcfg=$base/slcfg)
opts=$(IFS=, ; echo "${xargs[*]}")

mod_dir=$(dirname $(which $prog))/../lib/wokfs

rundaemon $filter $prog -L "insert 0 $mod_dir/$mod $opts" -U $mp
