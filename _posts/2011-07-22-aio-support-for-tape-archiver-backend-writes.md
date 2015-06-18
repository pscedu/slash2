---
layout: post
title: AIO support for tape archiver backend writes
author: pauln
type: progress
---

Jared has been working on asynchronous I/O support so that users may read from a tape-based SLASH2 I/O service.  The patches work around RPC timeout issues by immediately replying to the initial request and calling back the client once the I/O server has filled the buffer from tape.  While the data is being fetched from tape, the client and I/O server threads do not block so that other application requests may be serviced.

In testing some of these new patches I've noticed that writes issued by clients to IOS's with type "archival_fs" are having some issues.  The reason is because the SLIOD tries to fault in the first 1MB of the file before processing the incoming write buffer.  The entire sliver is needed to calculate the checksum – similar in nature to a read-modify-write on a RAID system.  Presumably, if the incoming write request were for a full sliver this "read-prep-write" wouldn't be necessary but these are 64k writes.  The plan was to not implement async I/O for writes but it would seem that we have no choice.

The question for the moment is where to hold the write buffer.  When the write hits the SLIOD it's presumably accompanied by a valid bmap lease.   If the SLIOD were to return <tt>EWOULDBLOCK</tt> to the client (and not take the bulk), the client would be forced to retry later.  This retry could even be prompted via a callback once the SLIOD has readied the respective sliver buffer.  However this method leads us to the problem of dirty buffers sitting on the client which are covered by expiring bmap leases.  While the bmap leases may be refreshed this approach is still problematic because it makes the client more susceptible to being caught with dirty buffers and no valid lease by which to flush them.  I feel like this situation should always be avoided when possible.  

Another method would be to put the heavy lifting onto SLIOD.  This would avoid the problem of delaying the client while it holds dirty data:
<ol>
<li>client sends write</li>
<li>sliod sees <tt>EWOULDBLOCK</tt> on sliver fault</li>
<li>sliod performs bulk, taking the buffer from the client but returns <tt>EWOULDBLOCK</tt> to the client</li>
<li>sliod attaches the write buffer to the pending aio read</li>
<li>the client fsthr handles this op just like an aio read - queuing the fuse reply so that it may be handled once we're notified by the SLIOD</li>
</ol>

Would direct I/O mode do the trick?  That would simplify things a bit by removing cached pages from equation but the SLIOD will still have to perform a read-prep-write if the write doesn't cover an entire sliver.  

