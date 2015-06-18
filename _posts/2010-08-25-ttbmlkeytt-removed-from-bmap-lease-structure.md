---
layout: post
title: <tt>bml_key</tt> removed from bmap lease structure
author: pauln
type: progress
---

To addressing yesterday's bmap lease bug, I've decided to remove the
<tt>bml_key</tt> and hence, bml association from the odtable entry.
At this point the odtable key is only known by the bmdsi.
Hopefully this will properly address issues with out of order lock releases.
