---
layout: post
title: Batch processing of replication requests
author: yanovich
type: progress
---

A few days ago, I committed bits to convert the replication arrangement
engine in the MDS to use batch RPC processing.
This gobbles up a bunch of tiny requests intended to a destination IOS
into a single RPC and blasts it off, allowing for more effective
throughput by less RPC overhead, especially for many small files.
