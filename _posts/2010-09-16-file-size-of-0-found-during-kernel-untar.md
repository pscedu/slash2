---
layout: post
title: File size of 0 found during kernel untar
author: pauln
type: progress
---

I think this is due to several restarts of the MDS while I/O to the <tt>sliod</tt> was ongoing.  Still tracking.

For now here's a simple regression test to track this:

cli1:

<pre>
(cd /tmp/ && tar jxf ~pauln/linux-2.6.34.tar.bz2 && find linux-2.6.34 -type f -exec md5sum {} ; > /tmp/md5s)
</pre>

cli2:

<pre>
(~pauln/s2reg/kernel.sh > /dev/null)
</pre>

cli1:

<pre>
(cd to kernel.sh outdir && md5sum -c /tmp/md5s)
</pre>
