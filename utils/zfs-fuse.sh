#!/bin/sh
# $Id$

set -e

full=$(readlink -f $0)
dir=$(dirname $full)
cd $dir
ZFS_BASE=$(make printvar-ZFS_BASE)

${ZFS_BASE}/zfs-fuse/run.sh -c 0 "$@"
