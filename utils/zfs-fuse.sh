#!/bin/sh
# $Id$

set -e

full=$(readlink -f $0)
dir=$(dirname $full)
cd $dir
ZFS_BASE=$(make printvar-ZFS_BASE)

umount -f /zfs-kstat || true
mkdir -p /zfs-kstat || true

exec ${ZFS_BASE}/src/zfs-fuse/run.sh -c 0 "$@"
