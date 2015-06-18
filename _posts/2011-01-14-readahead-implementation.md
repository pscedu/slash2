---
layout: post
title: Readahead Implementation
author: pauln
type: progress
---

In an effort to improve read performance, I've been working on patches to dynamically prefetch data when a file's I/O access pattern is determined to be sequential.  First attempts at implementation were partially successful.  The readahead (RA) logic was able to prefetch relevant pages into the client's cache.  However, the performance increase was not as large as had been expected.  Slash2 writes can easily saturate a gige connection, and on a loopback'ed host running client and sliod, I've seen >300MB/s writes. (Note: performance here was disk bound).

Reads are typically in the range of 60 MB/s, and with RA, about 75MB/s.  My working theory is that read prefetches must not block the requesting client thread for longer than necessary.  As expected, most of the latency involved in transferring large chunks is due to the link bandwidth.  For gige, it takes about 8ms to transfer a 1MB buffer.  In the current implementation, if the client requested 32kb it waits for the entire 1MB RA I/O to complete.  In this case, the thread sits idle for about 7ms.  My hope was for this cost to be sufficiently amortized by subsequent read I/O's so that a simple, synchronous RA model could be employed.  Testing has revealed this is not the case..  My first attempt at an async model will be done without the use of a dedicated thread.  
