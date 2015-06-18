---
layout: post
title: Clean up direct I/O (DIO) mechanism
author: zhihui
type: progress
---

Whenever two or more clients have conflicting access requests on the same block within the same file (e.g., two write requests), the SLASH2 MDS must force all existing lease holders to downgrade to the so-called direct I/O (DIO) mode. In the DIO mode, every client writes and reads directly to or from the IO server, bypassing its local page cache. This mechanism mostly works, but there were some lingering issues.

First, we must not hand out a DIO lease until after all existing lease holders have replied.  Second, a new lease request must honor the DIO transitional period.  Just because a block is concurrently not in the DIO mode does not mean a non-conflicting lease request should get a caching lease.  Of course, devels are always in the details. While in theory one client should have one lease per block per file, but it can have more than one lease in flight (due to RPC delays).  The MDS should handle that properly.

After this clean up, all the known corner cases are ironed out.  The number of codes actually reduced as a result.
