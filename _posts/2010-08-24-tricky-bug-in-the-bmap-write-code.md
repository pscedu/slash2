---
layout: post
title: Tricky bug in the bmap write code 
author: pauln
type: progress
---

Iozone does some interesting things to SLASH2.
By holding bmaps open for long periods of time and performing chwrmode
on read bmaps, iozone has been shaking out many types of bugs -
especially in the bmap lease code.

Here's a case where <tt>sliod</tt> relinquishes his most recent lease
for <tt>bmap@0x15e0c10</tt> while another duplicate write lease remains.
Since the duplicate is an older write lease, it doesn't have the correct
odtable key for releasing the odtable slot.
Soon the MDS will crash because <tt>mdsi_writers == 0</tt> but the mion
is still assigned.

This bug is partially caused by the fact that <tt>sliod</tt> only
remembers one release seq number per bmap.
Subsequent release requests of write bmaps overwrite any previously
stored sequence number.
While this seems buggy, it in fact helps simulate downed or otherwise
unreachable sliods.
Therefore the problem must be dealt with by the MDS.

One suggestion is that for revocation of a newer write lease, all older
write leases are invalidated too.
This would solve the problem of the
odtable entry key.

<pre class='code'>
[1282675290:846711 slmrmithr03:7617:bmap:mds_handle_rls_bmap:1011] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=4 release 4294969716 nid=562995062530842 pid=2147502232 bml=0x15d4820
[1282675290:846742 slmrmithr03:7617:bmap:mds_bmap_bml_release:774] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=4 bml=0x15d4820 seq=4294969716 key=-3267403435540628159
[1282675290:846761 slmrmithr03:7617:bmap:mds_bmap_dupls_find:464] bmap@0x15e0c10 b:1 m:1024 i:80000002d9347 opcnt=4 bml=0x15d4c40 tmp=0x15d4c40 (wlease=1 rlease=0) (nwtrs=1 nrdrs=0)
[1282675290:846775 slmrmithr03:7617:bmap:mds_bmap_dupls_find:464] bmap@0x15e0c10 b:1 m:1024 i:80000002d9347 opcnt=4 bml=0x15d4c40 tmp=0x15d4820 (wlease=2 rlease=0) (nwtrs=1 nrdrs=0)
[1282675290:846791 slmrmithr03:7617:bmap:mds_bmap_bml_release:950] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=4 removing reference (type=2)
[1282675290:846807 slmrmithr03:7617:bmap:mds_handle_rls_bmap:1019] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=3 removing reference (type=0)
[1282675295:139646 slmbmaptimeothr:7613:bmap:mds_bmap_bml_release:774] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=2 bml=0x15d4c40 seq=4294969689 key=8707359475757897713
[1282675295:139661 slmbmaptimeothr:7613:bmap:mds_bmap_dupls_find:464] bmap@0x15e0c10 b:1 m:1024 i:80000002d9347 opcnt=2 bml=0x15d4c40 tmp=0x15d4c40 (wlease=1 rlease=0) (nwtrs=1 nrdrs=0)
[1282675295:139677 slmbmaptimeothr:7613:bmap:mds_bmap_bml_release:894] bmap@0x15e0c10 b:1 m:1024 i:80000002d9347 opcnt=2 bml=0x15d4c40 bmdsi_writers=0 bmdsi_readers=0
[1282675295:139697 slmbmaptimeothr:7613:bmap:mds_bmap_bml_release:928] bmap@0x15e0c10 b:1 m:1024 i:80000002d9347 opcnt=2 !bmdsi_writers but bml_key (8707359475757897713) != odtr_key(-3267403435540628159)
[1282675295:139711 slmbmaptimeothr:7613:bmap:mds_bmap_bml_release:950] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=2 removing reference (type=2)
[1282675299:523693 slmrmcthr25:7666:bmap:bmap_lookup_cache_locked:123] bmap@0x15e0c10 b:1 m:0 i:80000002d9347 opcnt=2 took reference (type=0)
</pre>
