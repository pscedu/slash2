#!/usr/bin/env bash
# $Id$

prog=slashd
ctl=slmctl

. ${0%/*}/pfl_daemon.sh

usage()
{
	echo "usage: $0 [-gs]" >&2
	exit 1
}

while getopts "gs" c; do
	case $c in
	g) mygdb='mygdb'	;;
	s) mystrace='strace'	;;
	*) usage		;;
	esac
done
shift $(($OPTIND - 1))

apply_host_prefs "$@"
[ -n $notfound ] && cat <<EOF
no profile for this host, assuming defaults.
create a profile and send to archproj@psc.edu
EOF

[ $# -gt 0 ] && usage

base=$dir/$prof.s2
# Initialization/configuration
ulimit -n 100000
ulimit -c $((1024 * 1024 * 1024 * 50))
sysctl -w vm.max_map_count=500000
export LD_LIBRARY_PATH=/usr/local/lib
export PSC_SYSLOG=1
export PSC_LOG_LEVEL=warn
export PSC_LOG_LEVEL_info=info
export PSC_LOG_FILE=$base/log/$host.$name/%t
export PSC_LOG_FILE_LINK=$base/log/$host.$name/latest
export CONFIG_FILE=$base/slcfg

type modprobe >/dev/null 2>&1 && modprobe fuse

do_exec

$mystrace $mygdb $prog -D $base/var
[ $? -eq 0 ] && trap '' EXIT && exit

postproc

sleep 10
exec $0 "$@"
