---
layout: post
title: Fixed major memory leak in slashd (r5428)
author: pauln
type: progress
---

So this bug was preventing us from performing large scale namespace testing.
Since the patch I've run two of my standard namespace stress tests (from two different nodes).
The test from grapefruit completed successfully - all 5 million files were created and unlinked successfully.
The same test from lime, which is a slightly faster node, hung during the unlink phase.
I think due to a deadlock in the client.  Here's what I got from <tt>gstack(1)</tt>:

<pre>
Thread 6 (Thread 1291950400 (LWP 27028)):
#0  0x00002ba18e7d44a1 in nanosleep () from /lib64/libpthread.so.0
#1  0x00000000004234b9 in spinlock ()
#2  0x0000000000426aad in __fidc_lookup_fg ()
#3  0x0000000000426679 in fidc_put_locked ()
#4  0x0000000000424204 in fidc_reap ()
#5  0x0000000000424cd1 in fidc_get ()
#6  0x0000000000428478 in __fidc_lookup_inode ()
#7  0x00000000004044e0 in slash2fuse_fidc_putget ()
#8  0x00000000004045fa in slash2fuse_fidc_put ()
#9  0x000000000040f29a in slash2fuse_lookuprpc ()
#10 0x000000000041089c in slash2fuse_lookup_helper ()
#11 0x0000000000403a06 in slash2fuse_listener_loop ()
#12 0x00002ba18e7cd305 in start_thread () from /lib64/libpthread.so.0
#13 0x00002ba18eaae50d in clone () from /lib64/libc.so.6
#14 0x0000000000000000 in ?? ()

Thread 4 (Thread 1308735808 (LWP 27030)):
#0  0x00002ba18e7d44a1 in nanosleep () from /lib64/libpthread.so.0
#1  0x00000000004234b9 in spinlock ()
#2  0x0000000000423459 in reqlock ()
#3  0x0000000000426af1 in __fidc_lookup_fg ()
#4  0x000000000042735f in fidc_lookup_fg ()
#5  0x00000000004274a0 in __fidc_lookup_inode ()
#6  0x000000000040f380 in slash2fuse_lookup_helper ()
#7  0x0000000000403a06 in slash2fuse_listener_loop ()
#8  0x00002ba18e7cd305 in start_thread () from /lib64/libpthread.so.0
#9  0x00002ba18eaae50d in clone () from /lib64/libc.so.6
#10 0x0000000000000000 in ?? ()
</pre>

I think this has been fixed by https://mugatu.psc.edu/bugzilla/show_bug.cgi?id=13.

