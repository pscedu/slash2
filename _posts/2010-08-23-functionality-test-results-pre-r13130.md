---
layout: post
title: Functionality test results (pre-r13130)
author: pauln
type: progress
---

<ul class='expand'>
<li>STWT_SZV - SUCCESS</li>
<li>MTWT_SZV - SUCCESS</li>
<li>IOZONE - FAIL</li>
<li>CREATE+STAT - NOTRUN</li>
</ul>

<h5>Today's bugfixes</h5>

Have uncommitted patches on grapefruit to deal with a problem spotted by
Zhihui during a kernel compile.
The problem was that the client was releasing a write bmap prior to
doing any I/O.
Shortly after the client issues I/O but <tt>sliod</tt> had already scheduled the
bmap to be released.
The bmap does not get released by <tt>sliod</tt> because pending I/O's are
detected but once the final bcr has been committed to the MDS.
<tt>sliod</tt> then tries to schedule the bmap for release ops but fails
because it had already been placed on the queue.

Testing patches for a chwrmode bug on the MDS where a mode change
blindly increments <tt>bmdsi_writers</tt>, even if that client has a duplicate
write lease.
