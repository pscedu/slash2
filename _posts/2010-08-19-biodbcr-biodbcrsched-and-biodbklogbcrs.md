---
layout: post
title: biod_bcr, biod_bcr_sched, and biod_bklog_bcrs
author: pauln
type: progress
---

Retooling this a bit.  The current code was last touched during the mad rush for TG'10.

<dl>
<dt><tt>biod_bcr</tt></dt>
<dd>The pointer to the most recent, non-full bcr for the given biodi.</dd>
<dt><tt>biod_bcr_sched</tt></dt>
<dd>Means that a bcr is either on the hold or ready list.</dd>
<dt><tt>biod_bklog_bcrs</tt></dt>
<dd>Overflow queue for bcrs waiting to be sent to MDS or for more CRCs.
bcr's not at the tail of the list must be full.</dd>
</dl>

Bcr processing is tricky because at any given time, sliod can only send a single
bcr to the MDS.  A bcr must be on a list.. either the hold, ready, or backlog.
