---
layout: post
title: New client READDIR
author: yanovich
type: progress
---

<tt>READDIR</tt> in the client has been rewritten for performance considerations.  The major changes are:

<ul>

<li> avoid an additional round-trip (RTT) <b>after</b> the last "page" of direntries has been fetched denoting EOF.  Previously, READDIR in the SLASH2 client mount_slash exactly modeled the <tt>getdents(2)</tt> system calls performed by the process, an arrangement unnecessary and foolhardy considering the remote/network nature of the SLASH2 client <-> MDS communication. </li>

<li> asynchronous readahead for the next expected page after a <tt>getdents(2)</tt> is issued.  This is capped within certain limits so a <tt>readdir(3)</tt> on a huge directory does not exhaust memory in the client (or MDS for that matter).  The implementation issues another <tt>READDIR</tt> after one finishes in the client for big size to take advantage of throughput during huge directory reads but again respecting memory concerns.  Because of the strange nature of dirent offsets, the readahead is issued only after the current direntries page finishes, as the dirent offset in many modern file systems reflects a cookie for traversal instead of a physical offset as the on-disk format may be a non-linear data structure such as a B-tree.  In the case of the backing MDS file system, ZFS, a cookie is used but with certain properties that shouldn't cause issues with the heuristics in the client readdir direntry buffering cache. </li>

<li> pages of direntries are now cached.  Much in the style of file <tt>stat(2)</tt> attributes in SLASH2, pages are held around after a <tt>getdents(2)</tt> for other applications instead of being immediately marked for release.  This cached data is reclaimed on-demand when needed and not periodically later like in the old code, which can be resurrected if necessary very easily.  Operations such as timeout (exactly like the file <tt>stat(2)</tt> attribute caching) or anything such as <tt>rename(2)</tt>, <tt>creat(2)</tt>, <tt>unlink(2)</tt>, <tt>symlink(2)</tt>, etc. immediately remove dircache pages to avoid inconsistency errors.</li>

<li> negative extended attributes are now cached.  Modern Linux applications such as <tt>ls(1)</tt> perform <tt>listxattrs(2)</tt> which adds another synchronous RPC to each dirent returned in <tt>getdents(2)</tt>.  The MDS now performs this on each entry before replying, returning only the number of extended attributes for each file, and a flag is set in the client when this number is zero so it is known not to bother querying the MDS for this information again soon when the application shortly after <tt>getdents(2)</tt> finishes when issuing the <tt>listxattrs(2)</tt> on each entry returned.</li>

</ul>

With these improvements, the speed of <tt>readdir(3)</tt> really flies!
