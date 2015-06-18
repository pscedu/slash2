---
layout: post
title: Garbage collection improvements
author: yanovich
type: progress
---

Zhihui has recently fixed a number of issues in the garbage reclamation code contained within the MDS that was preventing large gaps between IOSes from catching up to each other.  Obviously on large deployments this becomes a major factor of concern.

Furthermore, a number of other improvements in the handling of garbage reclamation updates to IOSes were made such as general RPC robustness, selection, error log spam on error, and error discovery, and progress in the event of errors.

Finally, a new API was added that will in the near future convert the structure of the garbage reclamation update RPCs to use an asynchronous mechanism as large updates on slow hardware (such as tape archivers) can effectively timeout the RPC response and look like a network error.
