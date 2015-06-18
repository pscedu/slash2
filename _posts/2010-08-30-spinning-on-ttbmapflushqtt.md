---
layout: post
title: Spinning on <tt>bmapFlushQ</tt>
author: pauln
type: progress
---

Pending writes should be refined with a field for unsent writes.
This way the bmap flush thread would not keep reprocessing bmaps which
have previously sent, uncompleted write biorqs.
At the moment the bmap flush thread polls the flushQ, sleeping for 2&micro;s.
