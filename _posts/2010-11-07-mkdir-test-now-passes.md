---
layout: post
title: mkdir test now passes
author: pauln
type: progress
---

Testing a patch which fixes an attribute bug which prevents a parent directory from having its time attrs updated in the slash2 client and mds fcmh cache.

<pre class='code'>
(pauln@born-of-fire:pauln)$ (cd /s2/pjd-fstest-20080816/ && sudo prove -r tests/mkdir)
tests/mkdir/00.t .. ok     
tests/mkdir/01.t .. ok   
tests/mkdir/02.t .. ok   
tests/mkdir/03.t .. ok     
tests/mkdir/04.t .. ok   
tests/mkdir/05.t .. ok     
tests/mkdir/06.t .. ok     
tests/mkdir/07.t .. ok   
tests/mkdir/08.t .. ok   
tests/mkdir/09.t .. ok   
tests/mkdir/10.t .. ok     
tests/mkdir/11.t .. ok   
tests/mkdir/12.t .. ok   
All tests successful.
Files=13, Tests=105,  3 wallclock secs ( 0.11 usr  0.02 sys +  3.72 cusr  0.31 csys =  4.16 CPU)
Result: PASS
</pre>
