#!/bin/sh
# $Id$

set -e

base=$(pwd)/$(dirname $0)

cd $base 2>/dev/null
ZFS_BASE=$(make printvar-ZFS_BASE)

${ZFS_BASE}/cmd/zpool/zpool "$@"
