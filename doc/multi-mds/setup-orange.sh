#!/bin/bash
#
# 03/18/2012: setup-orange.sh
#

umount /zhihui_slash2_orange
umount /zhihui_slash2_orange/orange

killall zfs-fuse

rm -rf /zhihui_slash2_orange/*
rm -rf /zhihui_slash2_orange/.slmd

rm /home/zhihui/zhihui_slash2_orange.cf

sleep 5

export ZFS_SOCK_NAME=/var/run/zfs/zfs_socket_zhihui
export ZFS_LOCK_FILE=/var/lock/zfs/zfs_lock_zhihui

/home/zhihui/projects-orange-manyfs/zfs/src/zfs-fuse/zfs-fuse

sleep 5

/home/zhihui/projects-orange-manyfs/zfs/src/cmd/zpool/zpool destroy zhihui_slash2_orange
/home/zhihui/projects-orange-manyfs/zfs/src/cmd/zpool/zpool create -f zhihui_slash2_orange mirror sdc sdd
/home/zhihui/projects-orange-manyfs/zfs/src/cmd/zpool/zpool set cachefile=/home/zhihui/zhihui_slash2_orange.cf zhihui_slash2_orange
/home/zhihui/projects-orange-manyfs/zfs/src/cmd/zfs/zfs create zhihui_slash2_orange/orange

/home/zhihui/projects-orange-manyfs/slash_nara/slimmns/slimmns_format  -u 1234567812345678 /zhihui_slash2_orange
/home/zhihui/projects-orange-manyfs/slash_nara/slimmns/slimmns_format  -u 1234567812345679 -I 123 /zhihui_slash2_orange/orange

# Check out new-created files
 
find /zhihui_slash2_orange -maxdepth 3

sync
sleep 10

echo "File system IDs:"

cat /zhihui_slash2_orange/.slmd/siteid
cat /zhihui_slash2_orange/.slmd/fsuuid
cat /zhihui_slash2_orange/orange/.slmd/siteid
cat /zhihui_slash2_orange/orange/.slmd/fsuuid

umount /zhihui_slash2_orange/orange
umount /zhihui_slash2_orange

/home/zhihui/projects-orange-manyfs/slash_nara/slmkjrnl/slmkjrnl -f -b /dev/sdg1 -n 4096 -u 1234567812345679

killall zfs-fuse

