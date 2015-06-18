---
layout: post
title: sliod valgrind run
author: pauln
type: progress
---

<pre class='code'>
==15268== 
==15268== HEAP SUMMARY:
==15268==     in use at exit: 163,287,806 bytes in 40,358 blocks
==15268==   total heap usage: 1,653,889 allocs, 1,613,531 frees, 803,951,246 bytes allocated
==15268== 
==15268== LEAK SUMMARY:
==15268==    definitely lost: 0 bytes in 0 blocks
==15268==    indirectly lost: 0 bytes in 0 blocks
==15268==      possibly lost: 13,876,803 bytes in 36,714 blocks
==15268==    still reachable: 149,411,003 bytes in 3,644 blocks
==15268==         suppressed: 0 bytes in 0 blocks
==15268== Rerun with --leak-check=full to see details of leaked memory
==15268== 
==15268== For counts of detected and suppressed errors, rerun with: -v
==15268== ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 4 from 4)
</pre>
