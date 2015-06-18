---
layout: post
title: Recent rework of client I/O infrastructure
author: zhihui
type: progress
---

The disk-based archive system at PSC has been in production for about a year. Overall, the system works for the most part. However, there was a period when we saw more client crashes than we would like when the system was under serious load or the I/O service was down. Initially, we tried to tackle these issues in baby steps until one day we realized that perhaps a major rewrite is needed.

One problem with the old code is that it uses a lot of locking and flags to make sure key data structures will not be freed prematurely.  With enough mental gynmastics, we can convince ourselves that the old code works. However, a simpler and more robust way is to use reference count. Another problem with the old code is that it has the same logic duplicated at different places. 

So the client rewrite is actually a major clean up, using reference counts to protect key data structures and consolidating logics. After this clean up, regardless whether a request is split into multiple RPCs or it is done via AIO or it needs to be retired, the same code path is used. The new code also does not assume when a PRC will be complete.

Any newly written code is about to have bugs, especially when the new code changes a lot of assumptions that the old code relies on. Over the past month, we have dealt with a few fallouts of the new code.  And the client code seldom crashes these days.

