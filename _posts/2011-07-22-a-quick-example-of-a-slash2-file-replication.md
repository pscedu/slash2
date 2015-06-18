---
layout: post
title: A quick example of a SLASH2 file replication
author: pauln
type: progress
---

This example was given during the <a href="http://teragrid.org/tg11/ ">TG'11</a> <a href="papers/tg_2011_slash2.pdf">talk</a> but I thought it may be of interest so I'm copying the results here.

First, create a file to replicate:
<pre class='code'>
(pauln@peel0:~)$ dd if=/dev/zero of=/p0_archive/pauln/big_file count=2k bs=1M
2048+0 records in
2048+0 records out
2147483648 bytes (2.1 GB) copied, 4.81828 seconds, 446 MB/s
</pre>

Next, view its status with <a href="mdoc.pwl?q=msctl;sect=8">msctl</a>:
<pre class='code'>
(pauln@peel0:msctl)$ ./msctl -r /p0_archive/pauln/big_file
file-replication-status                                       #valid #bmap %prog
================================================================================
/p0_archive/pauln/big_file
new-bmap-repl-policy: one-time
  archsliod@PSCARCH                                              16    16    100%
    ++++++++++++++++
</pre>

Request replication of the entire file:
<pre class='code'>
(pauln@peel0:msctl)$ date && ./msctl -Q archlime@PSCARCH:*:/p0_archive/pauln/big_file
Wed Jul 20 02:51:56 EDT 2011
(pauln@peel0:msctl)$
</pre>

Now check the status with <a href="mdoc.pwl?q=msctl;sect=8">msctl</a>:
<pre class='code'>
(pauln@peel0:msctl)$ date && ./msctl -r /p0_archive/pauln/big_file
Wed Jul 20 02:51:57 EDT 2011
file-replication-status                                        #valid #bmap %prog
=================================================================================
/p0_archive/pauln/big_file
new-bmap-repl-policy: one-time
  archsliod@PSCARCH                                                16   16  100%
    ++++++++++++++++
  archlime@PSCARCH                                                  0   16     0%
    sqqqqqqqqqqqqqqq

(pauln@peel0:msctl)$ date && ./msctl -r /p0_archive/pauln/big_file
Wed Jul 20 02:52:05 EDT 2011
file-replication-status                                        #valid #bmap %prog
=================================================================================
/p0_archive/pauln/big_file
new-bmap-repl-policy: one-time
  archsliod@PSCARCH                                                16   16   100%
    ++++++++++++++++
  archlime@PSCARCH                                                 10   16 62.50%
    ++++++++++qqqqqq

(pauln@peel0:msctl)$ date && ./msctl -r /p0_archive/pauln/big_file
Wed Jul 20 02:52:20 EDT 2011
file-replication-status                                        #valid #bmap %prog
=================================================================================
/p0_archive/pauln/big_file
new-bmap-repl-policy: one-time
  archsliod@PSCARCH                                                16   16   100%
    ++++++++++++++++
  archlime@PSCARCH                                                 16   16   100%
    ++++++++++++++++
</pre>









