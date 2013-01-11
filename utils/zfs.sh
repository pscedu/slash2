#!/bin/sh
# $Id$

set -e

full=$(readlink -f $0)
base=$(dirname $full)
cd $base
ZFS_BASE=$(make printvar-ZFS_BASE)

exec ${ZFS_BASE}/src/cmd/zfs/zfs "$@"
