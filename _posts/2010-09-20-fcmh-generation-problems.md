---
layout: post
title: fcmh generation problems
author: pauln
type: progress
---

The method I'm exploring now is one where the clients will flush their
bmap caches and release bdbufs for bmaps belonging to the truncated FID.
We must ensure that no async I/O's issued prior to truncate are sent to
sliod before acquiring new bdbufs.
Therefore we must lock the fcmh and iteratively free its bmaps prior to
sending any truncate RPC to the MDS.
