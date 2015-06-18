---
layout: post
title: Full truncate / generation numbering bug
author: pauln
type: progress
---

Full truncate causes the FID's generation number to be increased.  This is mainly for garbage collection purposes so that <tt>sliod</tt> objects may be garbage collected asynchronously.  Upon being presented with a new FID/gen pair, <tt>sliod</tt> will create a new object.  When dealing with full truncate this technique gets tricky.  In single writer mode, we must ensure that queued writes, originating prior to the truncate are either not flushed at all and/or do not end up in the <tt>sliod</tt> object representing the new FID/gen pair.

In multi-writer mode each clients should be in directio mode, meaning that some relative order of operations can be implied.  Therefore bdbuf's with an old generation number may be targeted at the current backing file so long as a directio indicator is set.  A directio indicator should be similar to the one used for <tt>O_APPEND</tt> writes.

Any read, regardless of its bdbuf's generation number, should be targeted at the current <tt>sliod</tt> object.

For now, our client-side implementation of truncate will work like this:

<dl>
<dt>Full truncate:</dt>
<dd>All existing bmaps are "orphaned" so they may be replaced by bmaps with the most recent bdbuf tokens.  Pending or inflight I/O's may still be processed.  This is allowable because <tt>sliod</tt> will see that the generation number is old and that these are asynchronous writes.  Therefore they will be dropped.  The client could be made to cancel the flush operation for these writes too.</dd>

<dt>Partial truncate:</dt>
<dd>Block or cancel all writes past the truncation point so that asynchronous writes do not compromise the operational ordering.</dd>

<dt>Reads and DIO writes:</dt>
<dd>These always go the most recent object.</dd>
</dl>
