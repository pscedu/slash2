#!/bin/bash
#
# 03/18/2012: setup-citron.sh
#

umount /zhihui_slash2_citron
umount /zhihui_slash2_citron/citron

killall zfs-fuse

rm -rf /zhihui_slash2_citron/*
rm -rf /zhihui_slash2_citron/.slmd

rm /home/zhihui/zhihui_slash2_citron.cf

sleep 5

# The following two exports are needed to run two instances of mds on the same machine.

export ZFS_SOCK_NAME=/var/run/zfs/zfs_socket_zhihui
export ZFS_LOCK_FILE=/var/lock/zfs/zfs_lock_zhihui

/home/zhihui/projects-citron-manyfs/zfs/src/zfs-fuse/zfs-fuse
/home/zhihui/projects-citron-manyfs/zfs/src/cmd/zpool/zpool destroy zhihui_slash2_citron
/home/zhihui/projects-citron-manyfs/zfs/src/cmd/zpool/zpool create -f zhihui_slash2_citron sdl sdm sdn
/home/zhihui/projects-citron-manyfs/zfs/src/cmd/zpool/zpool set cachefile=/home/zhihui/zhihui_slash2_citron.cf zhihui_slash2_citron
/home/zhihui/projects-citron-manyfs/zfs/src/cmd/zfs/zfs create zhihui_slash2_citron/citron

/home/zhihui/projects-citron-manyfs/slash_nara/slimmns/slimmns_format  -u 1234567812345680 /zhihui_slash2_citron
/home/zhihui/projects-citron-manyfs/slash_nara/slimmns/slimmns_format  -u 1234567812345681 -I 456 /zhihui_slash2_citron/citron

# Check out new-created files

find /zhihui_slash2_citron -maxdepth 3

sync
sleep 10

echo "File system IDs:"

cat /zhihui_slash2_citron/.slmd/siteid
cat /zhihui_slash2_citron/.slmd/fsuuid
cat /zhihui_slash2_citron/citron/.slmd/siteid
cat /zhihui_slash2_citron/citron/.slmd/fsuuid

umount /zhihui_slash2_citron/citron
umount /zhihui_slash2_citron

/home/zhihui/projects-citron-manyfs/slash_nara/slmkjrnl/slmkjrnl -f -b /dev/sdo1 -n 4096 -u 1234567812345681

killall zfs-fuse

