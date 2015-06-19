---
layout: post
title: Functionality test results (r13105)
author: pauln
type: progress
---

<ul class='expand'>
<li>STWT_SZV - NOTRUN</li>
<li>MTWT_SZV - NOTRUN</li>
<li>IOZONE - FAILS with stack trace below</li>
<li>CREATE+STAT - NOTRUN</li>
</ul>

<pre>
Program received signal SIGSEGV, Segmentation fault.
[Switching to Thread 0x7ffebbfff710 (LWP 537)]
0x00000000004a2e26 in mds_bmap_directio (b=0x15e0c20, rw=SL_READ, np=0x15d3bd8)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/mds.c:207
warning: Source file is more recent than executable.
207                     psc_assert(bml->bml_flags & BML_WRITE);
(gdb) bt
#0  0x00000000004a2e26 in mds_bmap_directio (b=0x15e0c20, rw=SL_READ, np=0x15d3bd8)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/mds.c:207
#1  0x00000000004a63b6 in mds_bmap_bml_add (bml=0x15d3bc0, rw=SL_READ, prefios=131072)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/mds.c:597
#2  0x00000000004acd67 in mds_bmap_load_cli (f=0x8ecd30, bmapno=1, flags=0, rw=SL_READ,
    prefios=131072, sbd=0x7ffeb00028c0, exp=0x7ffe880010c0, bmap=0x7ffebbffe878)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/mds.c:1440
#3  0x00000000004c50be in slm_rmc_handle_getbmap (rq=0x19796d0)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/rmc.c:253
#4  0x00000000004ca59f in slm_rmc_handler (rq=0x19796d0)
    at /home/pauln/Code/fc13/projects/slash_nara/slashd/rmc.c:991
#5  0x000000000045b4f1 in pscrpc_server_handle_request (svc=0x183f210, thread=0x7ffeb00008c0)
    at /home/pauln/Code/fc13/projects/psc_fsutil_libs/psc_rpc/service.c:371
#6  0x000000000045e59b in pscrpcthr_main (thr=0x7ffeb00008c0)
    at /home/pauln/Code/fc13/projects/psc_fsutil_libs/psc_rpc/service.c:731
#7  0x0000000000489896 in _pscthr_begin (arg=0x7fffffffd4e0)
    at /home/pauln/Code/fc13/projects/psc_fsutil_libs/psc_util/thread.c:281
#8  0x0000003bf7e07761 in start_thread (arg=0x7ffebbfff710) at pthread_create.c:301
#9  0x0000003bf7ae14ed in clone () at ../sysdeps/unix/sysv/linux/x86_64/clone.S:115
</pre>
