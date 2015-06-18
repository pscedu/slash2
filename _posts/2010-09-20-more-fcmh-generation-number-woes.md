---
layout: post
title: More fcmh generation number woes
author: pauln
type: progress
---

It seems there's no good way for the MDS to inform the client, which
holds existing bdbuf's, that the generation number has changed.
<tt>sliod</tt> relies on the bdbuf to authenticate the I/O from the
client.
However, when full truncates occur, there's currently no good method
for adjusting the generation number within existing bdbufs held by the
client.
The result is that I/O's are being targeted to the incorrect backing
file on <tt>sliod</tt>.
