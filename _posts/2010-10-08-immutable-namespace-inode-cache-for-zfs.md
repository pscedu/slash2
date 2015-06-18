---
layout: post
title: Immutable Namespace Inode Cache for ZFS
author: pauln
type: progress
---

With rev 14128 we now have support for ultra fast lookups in the immutable namespace hierarchy.  A problem was spotted with zfs-fuse 0.6.9 where immns directory components were not being cached properly.  This resulted in disk-bound lookup operations which drastically slowed performance of file creates.  r14128 eliminates these lookups by prefetching the zfs inode numbers of immns directories prior to <tt>slashd</tt> startup.

<pre class='code'>
(pauln@lemon:slash_nara)$ svn ci ../zfs/src/zfs-fuse/zfs_operations_slash.c slashd/main_mds.c
Sending        slash_nara/slashd/main_mds.c
Sending        zfs/src/zfs-fuse/zfs_operations_slash.c
Transmitting file data ..
Committed revision 14128.
</pre>
