---
layout: post
title: MDS replication/update scheduler performance improvements
author: yanovich
type: progress
---

The update scheduler in charge of operations such as overseeing
replication has been changed to potentially schedule multiple
replication operations simultaneously, however necessary to saturate
bandwidth between I/O server pairs.

This should greatly improve the performance of replicating many small
files, as well as take advantage of long fat networks that would like to
pipeline more than one bmap (64MB) of data at a time.

The old scheme was a proof of concept that allowed a single bmap to be
in transmission between an I/O server pair fostering replication at a time.
Now, instead the system assigns a bandwidth limit between arbitrary I/O
server pairs and queues operations only when they would not exceed this
speed limit.

In the future, this algorithm will probably only need to be adjusted to more
accurately respect real dynamic limits between I/O servers spread
across sites, relating to their actual topologic characteristics, as
well as accomodate special needs such as network reservation by
administrator policy.
