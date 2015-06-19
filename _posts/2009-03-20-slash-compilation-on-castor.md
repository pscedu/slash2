---
layout: post
title: SLASH compilation on Castor
author: pauln
type: progress
---

Here are some changes I had to <tt>make</tt> for building on Fedora Core 10.
Note that a symlink was made in <tt>/usr/src/kernels</tt>.

<pre>
(psc@castor:projects)$ ls -al /usr/src/kernels/slash2-devel-kernel
lrwxrwxrwx 1 root root 30 Mar 20 13:04 /usr/src/kernels/slash2-devel-kernel -> 2.6.27.19-170.2.35.fc10.x86_64/


Index: psc_fsutil_libs/include/psc_util/atomic.h
===================================================================
--- psc_fsutil_libs/include/psc_util/atomic.h   (revision 5643)
+++ psc_fsutil_libs/include/psc_util/atomic.h   (working copy)
@@ -3,9 +3,12 @@
 #ifndef _PFL_ATOMIC_H_
 #define _PFL_ATOMIC_H_

+#include "psc_types.h"
 #include <sys/types.h>
-#include <asm/bitops.h>
-#include <asm/system.h>
+#include <asm/types.h>
+//#include <linux/bitops.h>
+//#include <asm/system.h>
+#include <asm/cmpxchg.h>

 #include <stdint.h>

</pre>
