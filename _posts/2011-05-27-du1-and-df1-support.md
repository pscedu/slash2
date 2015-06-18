---
layout: post
title: du(1) and df(1) support
author: yanovich
type: progress
---

Support for <tt>st_blocks</tt> in <tt>struct stat</tt> has been implemented to support utilities such as <tt>du(1)</tt> and <tt>statvfs(2)</tt> now more usefully reflects a <tt>mount_slash</tt> client's preferred I/O system for supporting utilities such as <tt>df(1)</tt>.

The <tt>statvfs(2)</tt> implementation simply tracks each I/O node's backend file system through updates sent to the MDS whenever convenient.  Then, when a <tt>mount_slash</tt> client issues a STATFS request to the MDS to get file system stats such as free blocks, the MDS returns the <tt>statvfs(2)</tt> pertaining to this client's preferred I/O system.

Future improvements: persistently store the <tt>statvfs</tt> data somewhere in the MDS.

The <tt>st_blocks</tt> implementation is a bit more involved but works in an somewhat analogous fashion: whenever a <tt>sliod</tt> sends CRC or REPLWK updates, which happens after I/O has been issued from the client or a replica has been made/updated, the <tt>st_blocks</tt> for the file involved is sent along to the MDS.  This value is tracked in the inode for each I/O system replica and, in the case of CRC updates, the delta from the previous value in the inode to the new value for this I/O system is applied to the file's <tt>st_blocks</tt>.

Specifically, <tt>st_blocks</tt> in SLASH2 means the number of 512-byte blocks in use by a file across all non-replicated regions of data, wherever these regions may reside.  Replicas update <i>only</i> their per-IOS count of blocks and not the aggregate <tt>st_blocks</tt> whereas WRITE updates affect <i>both</i> the per-IOS count as well as the aggregate <tt>st_blocks</tt> value returned to <tt>mount_slash</tt> clients.
