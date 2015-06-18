---
layout: post
title: New record for kernel untar and verify?
author: pauln
type: progress
---

<pre class='code'>
(pauln@orange:s2reg)$ ./kernel_full.sh

real	2m23.859s
user	0m15.368s
sys	0m2.746s

real	2m39.051s
user	0m1.036s
sys	0m1.693s
Success (/s2/1286779458)

real	5m2.926s
user	0m16.404s
sys	0m4.438s

(pauln@lemon:fuse)$ cat ~pauln/s2reg/kernel_full.sh
#!/bin/sh

TD=`date +%s`

time (mkdir -p /s2/${TD} && 
    cd /s2/${TD} && 
    time tar jxfv ~pauln/linux-2.6.34.tar.bz2  > /dev/null &&
    time md5sum -c ~pauln/linux-2.6.34.MD5s > /dev/null &&
    echo "Success (/s2/${TD})"
)
</pre>

