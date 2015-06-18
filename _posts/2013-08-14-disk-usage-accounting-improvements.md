---
layout: post
title: Disk usage accounting improvements
author: yanovich
type: progress
---

As it turns out, FreeBSD ZFS has some interesting behavioral oddities related to <tt>fsync(2)</tt> and <tt>st_blocks</tt>.  Specifically, after a <tt>write(2)</tt> then <tt>fsync(2)</tt>, the <tt>stat(2)</tt> <tt>st_blocks</tt> information returned does not immediately reflect the correct disk usage.

Without further investigation, cursory empirical discovery has revealed that this property is only updated after several seconds after the <tt>fsync(2)</tt>.  This behavior was accordingly implemented in <tt>sliod</tt> so the correct usage accounting information gets propagated to the MDS on bmap CRC updates (BCRs) instead of erroneous values.  The cost is that BCRs stay around in memory increasing memory pressure on busy IOSes and widening the window of lost BCRs to the MDS if the <tt>sliod</tt> fails.
