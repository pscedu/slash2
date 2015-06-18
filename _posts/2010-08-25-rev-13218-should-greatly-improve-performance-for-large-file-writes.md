---
layout: post
title: Rev 13218 should greatly improve performance for large file writes
author: pauln
type: progress
---

The buffer cache LRU reclamation was horribly buggy and inefficient.
Yesterday I determined that in some cases newer bmpce's were being
placed at the front of the list, causing older entries to be skipped (ie
not considered for reclamation).
Also found strong evidence that the bmpce lru's were ordered from
oldest to newest instead of the other way
around.
Preliminary test results with Iozone are promising.
Performance has jumped by a factor of 3 to 4!
