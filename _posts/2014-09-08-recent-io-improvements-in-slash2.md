---
layout: post
title: Recent I/O improvements in SLASH2
author: zhihui
type: progress
---

In the past few months, SLASH2 has received some impressive I/O
performance improvements for both reads and writes.

On the write side,  we used to do read-before-writes for misaligned
writes.
This hurts the performance of applications like genetorrent badly.
Luckily, we already have the logic to flush only the parts of the pages
that are actually dirty. Now we add a few new fields to each page that
track the area that has been written with new data.
That way, we don't have to read over the network anymore if an
application is only interested in writing data or if a read can be
satisfied by previously written data.
Two caveats:
* if a read has to go over the network, pending writes must be flushed
  first
* if a read over the network is in progress, a new write has to wait.
So the order matters.

On the read side, we used to have two problems.
The first problem is that we only launched read-ahead that is adjacent
to the end of the current read request.
In other words, we did not fill a pipe of pre-read pages for read
requests to catch up.
In the new code, the readahead window can be somewhere, say 4MiB, ahead
of the current read request.
This gives us a whopping 3-4 fold increase on some `dd` benchmark.
The second problem is that our readahead was only limited within a bmap.
The readahead logic is reset each time we cross the 128MiB bmap
boundary.
The new code uses a readahead thread to launch the readahead that is
beyond the current bmap.
This gives us a further 10% boost.
