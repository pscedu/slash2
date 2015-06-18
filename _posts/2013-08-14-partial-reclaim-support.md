---
layout: post
title: Partial reclaim support
author: yanovich
type: progress
---

Partial reclaim (not to be confused with partial truncation resolution) support has been added recently.

This entails sending notifications to IOSes that support <tt>fallocate(2)</tt> with the ability to release holes in backing files the signal to do so for data which has been released by SLASH2.

These instances may be from bmap replica ejection or <b>after</b> partial truncation has cleared some bmaps.  Partial truncation support is still not available as it requires some processing the IOS which must take provisions to ensure in-memory coherency.

The support for preclaim (partial reclaim), however, was very lightweight after the introduction of a general purpose asynchronous batch RPC API.  One remaining problem is the pruning of out-of-date preclaim updates which were added to the batch request at some point in the past which should no longer go out, such as in the case of <tt>TRUNCATE</tt>, <tt>WRITE</tt>; where a mistimed preclaim may clobber legitimate data received after the truncation.  A proposal tracking the bmap generation numbers may suffice for this issue.
