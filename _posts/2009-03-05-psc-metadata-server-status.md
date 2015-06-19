---
layout: post
title: PSC metadata server status
author: pauln
type: progress
---

<tt>wolverine</tt> (128.182.58.80) is configured as a SLASH2 metadata server.
Below is a dump of the ZFS configuration:

<pre>
(pauln@wolverine:zpool)$ sudo ./zpool status
  pool: wolverine_pool
 state: ONLINE
 scrub: none requested
config:

NAME        STATE     READ WRITE CKSUM
wolverine_pool  ONLINE       0     0     0
  raidz1    ONLINE       0     0     0
    sdb1    ONLINE       0     0     0
    sdc1    ONLINE       0     0     0
    sdd1    ONLINE       0     0     0

errors: No known data errors
</pre>

To date I've been able to successfully run a 10 million file create test (and unlink).
Here's the <tt>fio</tt> test config:

<pre>
group 8peReadWrite {
  files_per_dir = 1024;
  tree_depth    = 8;
  tree_width    = 3;
  pes           = 4;
  test_freq     = 0;
  block_freq    = 0;
  path          = /slashfs_client/pauln;
  output_path   = /tmp;
  filename      = 8paeRW_1mbs;
  file_size     = 128m;
  block_size    = 4m;
  thrash_lock   = yes;
  samedir       = yes;
  samefile      = no;
  intersperse   = no;
  seekoff       = no;
  fsync_block   = no;
  verify        = yes;
  barrier       = yes;
  time_block    = no;
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
WriteEmUp [create:openwr:close]
statem    [stat]
  )
}
</pre>
