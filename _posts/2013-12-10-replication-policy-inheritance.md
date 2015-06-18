---
layout: post
title: Replication policy inheritance
author: yanovich
type: progress
---

The HEAD branch of SLASH2 has been recently updated with support for user replication policy inheritance.  What this means is that via <tt>msctl(8)</tt>, a user can set preferred residency settings <i>on a directory</i> and all files (and subdirectories) created thereunder automatically inherit this policy.  This means no further commands are necessary to manage residencies after creation.  And full support for persistent replications is available via this mechanism as well.  Happy (easy) data managing!
