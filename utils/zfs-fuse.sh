#!/bin/sh
# $Id$

set -e

full=$(readlink -f $0)
dir=$(dirname $full)
cd $dir
ZFS_BASE=$(make printvar-ZFS_BASE)

exec ${ZFS_BASE}/src/zfs-fuse/run.sh -c 0 "$@"
