#!/usr/bin/env bash
# $Id$

prog=sliod
ctl=slictl

. $(dirname $0)/pfl_daemon.sh

usage()
{
	echo "usage: $0 [-dgOTv] [-F filter] [-P deployment-profile] [instance]" >&2
	exit 1
}

bkav=("$@")
while getopts "dF:gOPT:v" c; do
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

apply_host_prefs "$@"

base=$dir/$prof.s2

preinit

# Initialization/configuration
ulimit -n 100000
ulimit -c $((1024 * 1024 * 1024 * 50))
export LD_LIBRARY_PATH=/usr/local/lib
export PSC_SYSLOG=1
#export PFL_SYSLOG_PIPE=logger
export PSC_LOG_LEVEL=notice
export PSC_LOG_LEVEL_info=info
export PSC_LOG_FILE=${PSC_LOG_FILE:-$base/log/$host.$name/%t}
export PSC_LOG_FILE_LINK=$(dirname $PSC_LOG_FILE)/latest
export CONFIG_FILE=$base/slcfg

rundaemon $filter $prog -D $base/var
