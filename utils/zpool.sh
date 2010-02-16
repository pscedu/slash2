#!/bin/sh
# $Id$

set -e

full=$(readlink -f $0)
base=$(dirname $full)
cd $base
ZFS_BASE=$(make printvar-ZFS_BASE)

${ZFS_BASE}/cmd/zpool/zpool "$@"
