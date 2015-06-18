---
layout: post
title: MDS database performance improvements
author: yanovich
type: progress
---

Recently, work has been done to the MDS update scheduling engine.  Under the hood, an SQLite database is used to stage all that the MDS has to do.  This work involves scheduling replication activity, resolving partial <tt>truncate(2)</tt> blocks, and garbage reclamation.

Until now, an elementary method was used to store this work and retrieve it when convenient.  All execution threads inside the MDS wishing to do queries performed them in a shared mode of access no caching, placing limits on database performance.

The code has been restructured with the following improvements:
<ul>
<li>pass as many database queries as possible to a single thread to prevent cache flushing.</li>
<li>give each thread its own database handle to provide concurrent read operations.</li>
<li>move the database into RAM via tmpfs and <tt>/dev/shm</tt>.</li>
<li>backup the database occasionally in the event of a crash.</li>
</ul>

Obviously, between backups, there is a window for lost operations.  This issue will need to be addressed.
