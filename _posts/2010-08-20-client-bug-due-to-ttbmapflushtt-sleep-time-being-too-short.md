---
layout: post
title: Client bug due to <tt>bmap_flush()</tt> sleep time being too short
author: pauln
type: progress
---

To be clear this isn't exactly a bug but it does cause some CPU abuse:

<pre>
[1282327206:159214 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 restore to dirty list
[1282327206:161331 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 try flush (outstandingRpcCnt=75)
[1282327206:161347 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 try flush (outstandingRpcCnt=75)
[1282327206:161360 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 restore to dirty list
[1282327206:161374 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 restore to dirty list
[1282327206:163490 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 try flush (outstandingRpcCnt=75)
[1282327206:163507 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 try flush (outstandingRpcCnt=75)
[1282327206:163520 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 restore to dirty list
[1282327206:163533 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 restore to dirty list
[1282327206:165650 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 try flush (outstandingRpcCnt=75)
[1282327206:165668 msbflushthr:23667:bmap:bmap_flush:639] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 try flush (outstandingRpcCnt=75)
[1282327206:165682 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x881330 b:1 m:8259 i:80000002d907b opcnt=65 restore to dirty list
[1282327206:165700 msbflushthr:23667:bmap:bmap_flush:754] bmap@0x882110 b:3 m:8259 i:80000002d907b opcnt=19 restore to dirty list
</pre>

Every 2ms the <tt>bmap_flush</tt> thread wakes up to process new work.
The reason the thread's sleep is so short is so that it can service new
requests quickly.
For small files this is especially important because it lowers the
latency.
Perhaps another queue for delayed or pending I/O's would solve this
problem and allow immediate access to the queue with no 2ms spin.
