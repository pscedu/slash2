---
layout: post
title: AIO / Tape archiver backend support (cont'd)
author: pauln
type: progress
---

To support the upcoming SLASH2-based PSC data archiver, an AIO (asynchronous I/O) implementation was needed and is now largely complete.  As previously mentioned, AIO support in SLASH2 enables to the system to tolerate long delays between presentation of the initial I/O request and its completion.  Such support is needed for integration of tape archival systems as SLASH2 I/O systems.

The current implementation supports all types of I/O operations:  read, write, read-ahead, and replication.  The main caveat for the moment is that writes are forced to use direct I/O mode.  Complexities surrounding the management of bmap write leases and client-side buffer cache management were the primary factor here.  We definitely wanted to avoid creating scenarios where the client would be forced to 'juggle' write leases for arbitrary lengths of time.  Further, using cached I/O on the client may create quality of service problems for writes to a single tape resident file because I/O to this file may consume the entire write-back cache.  Due to the fact that writes may cause tape reads (as described in a previous post), this would cause the entire client to block for some arbitrary amount of time.   By using direct I/O these issues are completely avoided.

Both the client and sliod (aka I/O server) use a similar mechanism to deal with AIO'd buffers.  When it is determined that a request is dependent on AIO'd buffer for completion, that request is placed on a list attached to the buffer (on the sliod that buffer is a <tt>struct slvr_ref</tt>, on the client - <tt>struct bmap_pagecache_entry</tt>).  Upon completion of the AIO these buffers become 'READY' and any requests queued in the completion list are processed.  This method suits both client read I/O and sliod replication I/O equally well.  (Note: replication is more straight-forward since only a single buffer may be involved.)
