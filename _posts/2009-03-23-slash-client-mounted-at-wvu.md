---
layout: post
title: SLASH client mounted at WVU
author: pauln
type: progress
---

After a few go-rounds with various firewall and security mumbo-jumbo we
finally have mounted <tt>wolverine</tt>'s SLASH2 export at WVU.

Here's a <tt>df(1)</tt> command and <tt>gdb</tt> stack trace for the first bug:

<pre>
(root@castor:mount_slash)# df
Filesystem           1K-blocks      Used Available Use% Mounted on
/dev/sda3             68959192   5880592  59575628   9% /
/dev/sda1               101086     20617     75250  22% /boot
tmpfs                  1029476       640   1028836   1% /dev/shm
/slashfs_client      478468950    672976 477795975   1% /slashfs_client

Program received signal SIGABRT, Aborted.
[Switching to Thread 0x7fffe6be8950 (LWP 28034)]
0x0000003e8c232f05 in raise () from /lib64/libc.so.6
Missing separate debuginfos, use: debuginfo-install fuse-libs-2.7.4-2.fc10.x86_64 glibc-2.9-3.x86_64
(gdb) bt
#0  0x0000003e8c232f05 in raise () from /lib64/libc.so.6
#1  0x0000003e8c234a73 in abort () from /lib64/libc.so.6
#2  0x0000000000456477 in _psclogv (
    fn=0x4aef30 "..//..//psc_fsutil_libs/include/psc_util/lock.h",
    func=0x4aef25 "_tands", line=203, subsys=5, level=0, options=0,
    fmt=0x4aef80 "lock %p has invalid value (%d)", ap=0x7fffe6be78c0)
    at ..//..//psc_fsutil_libs/psc_util/log.c:225
#3  0x00000000004566af in _psc_fatal (
    fn=0x4aef30 "..//..//psc_fsutil_libs/include/psc_util/lock.h",
    func=0x4aef25 "_tands", line=203, subsys=5, level=0, options=0,
    fmt=0x4aef80 "lock %p has invalid value (%d)")
    at ..//..//psc_fsutil_libs/psc_util/log.c:246
#4  0x0000000000404f9b in _tands (s=0x7ffe58)
    at ..//..//psc_fsutil_libs/include/psc_util/lock.h:203
#5  0x0000000000404ebd in spinlock (s=0x7ffe58)
    at ..//..//psc_fsutil_libs/include/psc_util/lock.h:212
#6  0x0000000000404e58 in reqlock (sl=0x7ffe58)
    at ..//..//psc_fsutil_libs/include/psc_util/lock.h:255
#7  0x00000000004105f2 in slash2fuse_lookup_helper (req=0x7fffe0011460,
    parent=13240447, name=0x7fffe43a1038 "fio_f.pe6.8peRW_1mbs.0.35")
    at main.c:1099
#8  0x0000000000403ee2 in slash2fuse_listener_loop (arg=0x0)
    at fuse_listener.c:260
#9  0x0000003e8ce073da in start_thread () from /lib64/libpthread.so.0
#10 0x0000003e8c2e62bd in clone () from /lib64/libc.so.6
(gdb)
</pre>
