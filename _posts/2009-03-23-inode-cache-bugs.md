---
layout: post
title: Inode cache bugs
author: pauln
type: progress
---

Both this and the one below look to be caused by the reentrancy problem fixed in Zest about 2 weeks ago.

<pre>
[root@castor mount_slash]# fg
PSC_LOG_LEVEL=2 SLASH_MDS_NID="128.182.58.80@tcp10" LNET_NETWORKS="tcp10(eth1)" USOCK_CPORT=1300 gdb ./mount_slash

#0  0x0000000000404e0c in atomic_dec (v=0x28)
    at ..//..//psc_fsutil_libs/include/psc_util/atomic.h:393
#1  0x000000000040531a in slash2fuse_access (req=0x7fafb0, ino=15294024,
    mask=1) at main.c:229
#2  0x0000000000403ee2 in slash2fuse_listener_loop (arg=0x0)
    at fuse_listener.c:260
#3  0x0000003e8ce073da in start_thread () from /lib64/libpthread.so.0
#4  0x0000003e8c2e62bd in clone () from /lib64/libc.so.6
(gdb) up
#1  0x000000000040531a in slash2fuse_access (req=0x7fafb0, ino=15294024,
    mask=1) at main.c:229
229             fidc_membh_dropref(c);
(gdb) print *c
Cannot access memory at address 0x0
(gdb)
</pre>
