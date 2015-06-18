---
layout: post
title: Today's basic functionality tests (r12984)
author: pauln
type: progress
---

<ul>
<li>Single-threaded write test size verification from multiple clients (STWT_SZV) - PASS</li>
<li>Multi-threaded write test size verification from multiple clients - FAIL</li>
</ul>

<h5>Multi-threaded write test size verification from multiple clients</h5>

This is NOT working at log level 5.
Note these tests used the <tt>bessemer@PSC</tt> I/O backend, with 2 I/O nodes.

<pre class='code'>
group 8peReadWrite {
  files_per_dir = 4;
  tree_depth    = 0;
  tree_width    = 0;
  pes           = 4;
  test_freq     = 0;
  block_freq    = 0;
  path          = /s2/pauln;
  output_path   = /home/pauln/fio/tmp;
  filename      = largeioc;
  file_size     = 4g;
  block_size    = 1m;
  thrash_lock   = yes;
  samedir       = yes;
  samefile      = no;
  intersperse   = no;
  seekoff       = no;
  fsync_block   = no;
  verify        = yes;
  barrier       = yes;
  time_block    = yes;
  block_barrier = no;
  time_barrier  = no;
  iterations    = 1;
  debug_conf    = no;
  debug_block    = no;
  debug_memory    = no;
  debug_buffer    = no;
  debug_output    = no;
  debug_dtree     = no;
  debug_barrier   = no;
  debug_iofunc    = no;

  iotests (
	WriteEmUp [create:openwr:write:close]
  )
}
</pre>

All blocks were written:

<pre class='code'>
(pauln@lemon:TGFIO_tests)$ grep "block# 4095"  ./largeio.test1.outc
1282068769.436847 PE_00002 do_io() :: bl_wr 0000.090650 MB/s 0011.031429 block# 4095 bwait 00.000000
1282068775.774260 PE_00003 do_io() :: bl_wr 0000.088850 MB/s 0011.254921 block# 4095 bwait 00.000000
1282068776.913274 PE_00001 do_io() :: bl_wr 0000.083602 MB/s 0011.961443 block# 4095 bwait 00.000000
1282068778.415998 PE_00000 do_io() :: bl_wr 0000.075364 MB/s 0013.268957 block# 4095 bwait 00.000000
</pre>

However, all files should be 4294967296.  At least the clients agree on
the size which points to the mds or sliod as the culprit.

Orange:
<pre class='code'>
-rw-r--r-- 1 pauln staff 4215275520 Aug 17 14:07 fio_f.pe0.largeioc.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 17 14:07 fio_f.pe1.largeioc.0.0
-rw-r--r-- 1 pauln staff 4202037232 Aug 17 14:07 fio_f.pe2.largeioc.0.0
-rw-r--r-- 1 pauln staff 4215930864 Aug 17 14:07 fio_f.pe3.largeioc.0.0
</pre>

Lemon:
<pre class='code'>
-rw-r--r-- 1 pauln staff 4215275520 Aug 17 14:07 fio_f.pe0.largeioc.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 17 14:07 fio_f.pe1.largeioc.0.0
-rw-r--r-- 1 pauln staff 4202037232 Aug 17 14:07 fio_f.pe2.largeioc.0.0
-rw-r--r-- 1 pauln staff 4215930864 Aug 17 14:07 fio_f.pe3.largeioc.0.0
</pre>

<h5>Single threaded write test with size verification from multiple clients</h5>

This test <b>is</b> working at log level 5 on clients and servers.
<tt>stat(2)</tt>'s from the writer client and a 3rd party client are
both correct, with the 3rd party client timing out his size attributes
after 8 seconds.

<pre class='code'>
group 8peReadWrite {
  files_per_dir = 1;
  tree_depth    = 0;
  tree_width    = 0;
  pes           = 1;
  test_freq     = 0;
  block_freq    = 0;
  path          = /s2/pauln;
  output_path   = /home/pauln/fio/tmp;
  filename      = largeiob;
  file_size     = 4g;
  block_size    = 1m;
  thrash_lock   = yes;
  samedir       = yes;
  samefile      = no;
  intersperse   = no;
  seekoff       = no;
  fsync_block   = no;
  verify        = yes;
  barrier       = yes;
  time_block    = yes;
  block_barrier = no;
  time_barrier  = no;
  iterations    = 1;
  debug_conf    = no;
  debug_block    = no;
  debug_memory    = no;
  debug_buffer    = no;
  debug_output    = no;
  debug_dtree     = no;
  debug_barrier   = no;
  debug_iofunc    = no;

  iotests (
	WriteEmUp [create:openwr:write:close]
  )
}

Orange:
-rw-r--r-- 1 pauln staff 4294967296 Aug 17 13:58 fio_f.pe0.largeiob.0.0

Lemon:
-rw-r--r-- 1 pauln staff 4294967296 Aug 17 13:58 fio_f.pe0.largeiob.0.0
</pre>

Wow.  Been a while since I've updated this!
