---
layout: post
title: Filesizes for large files seem to be working again!
author: pauln
type: progress
---

Today's functionality test results (r13082)

<ul>
<li>STWT_SZV - PASS</li>
<li>MTWT_SZV - PASS</li>
<li>IOZONE - FAIL</li>
<li>CREATE+STAT ??</li>
</ul>

Lemon:
<pre class='code'>
Every 1.0s: ls -la /s2/pauln/FIO_TEST_ROOT/fio_f.pe0.largeioj.0.0 /s2/pauln/FIO_TEST_ROOT/fio_f.pe1.large...  Thu Aug 19 17:42:16 2010

-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe0.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe1.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe2.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe3.largeioj.0.0
</pre>

Orange:
<pre class='code'>
Every 1.0s: ls -la /s2/pauln/FIO_TEST_ROOT/fio_f.pe0.largeioj.0.0 /s2/pauln/FIO_TEST_ROOT/fio_f.pe1.large...  Thu Aug 19 17:42:16 2010

-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe0.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe1.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe2.largeioj.0.0
-rw-r--r-- 1 pauln staff 4294967296 Aug 19 17:27 /s2/pauln/FIO_TEST_ROOT/fio_f.pe3.largeioj.0.0
</pre>
