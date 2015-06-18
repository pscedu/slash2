---
layout: post
title: Sliod CRC Write Return Codes
author: pauln
type: progress
---

Just noticed that some contents of crc_update RPC's are not being processed at the MDS due to a failure of a single update element (<tt>srm_bmap_crcup</tt>).  I'm changing this so that each <tt>srm_bmap_crcup</tt> will have a return code representation in the RPC reply.
