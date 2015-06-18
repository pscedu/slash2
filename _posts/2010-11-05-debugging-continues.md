---
layout: post
title: debugging continues..
author: pauln
type: progress
---

Working on the mkdir test suite..

<pre class='code'>
(pauln@born-of-fire:pjd-fstest-20080816)$ prove tests/mkdir/
tests/mkdir/00.t .. 29/36 stat returned -1
[: 61: Illegal number: ENOENT
stat returned -1
[: 63: Illegal number: ENOENT
stat returned -1
[: 65: Illegal number: ENOENT
tests/mkdir/00.t .. Failed 18/36 subtests 
tests/mkdir/01.t .. ok   
tests/mkdir/02.t .. ok   
tests/mkdir/03.t .. ok     
tests/mkdir/04.t .. ok   
tests/mkdir/05.t .. Failed 7/12 subtests 
tests/mkdir/06.t .. Failed 7/12 subtests 
tests/mkdir/07.t .. ok   
tests/mkdir/08.t .. ok   
tests/mkdir/09.t .. ok   
tests/mkdir/10.t .. ok     
tests/mkdir/11.t .. ok   
tests/mkdir/12.t .. ok   

Test Summary Report
-------------------
tests/mkdir/00.t (Wstat: 0 Tests: 36 Failed: 18)
  Failed tests:  18-35
tests/mkdir/05.t (Wstat: 0 Tests: 12 Failed: 7)
  Failed tests:  4-10
tests/mkdir/06.t (Wstat: 0 Tests: 12 Failed: 7)
  Failed tests:  4-10
Files=13, Tests=105,  3 wallclock secs ( 0.10 usr  0.04 sys +  3.70 cusr  0.37 csys =  4.21 CPU)
Result: FAIL
</pre>
