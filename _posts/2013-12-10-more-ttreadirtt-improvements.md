---
layout: post
title: More <tt>READIR</tt> improvements
author: yanovich
type: progress
---

The previous rewrite of the SLASH2 client READDIR code, named <i>dircache</i>, which does the work of <tt>readdir(3)</tt> from user application system calls, unfortunately had some problems:

<ol>
  <li>qualitative value was expected from the underlying file system (ZFS)'s assignment of values to the <tt>d_off</tt> field.  This is a problem because modern file systems use this more as a cookie that does not confer position e.g. to traverse a B-tree.</li>
  <li>because we can't predict this number without advanced knowledge of the directory, which is by design only known by the MDS in SLASH2, there isn't really a great way to do readahead for improving the performance of reading all dirents from a large directory.  The old code had to wait an RTT for each chunk of dirents, with the slight exception of being able to issue the next one <i>after</i> the current one is replied.</li>
</ol>

So the code has been restructured to push this readahead responsibility onto the MDS.  The MDS now sets up multiple RPCs when one READDIR request comes in in anticipation that the client will soon request the new chunk of dirents.  This comes at the cost of putting some additional load on the MDS but the advantage should be clear and really helps and the the readahead is currently limited to three simultaneous RPCs.
