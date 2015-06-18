---
layout: post
title: More testing.. this time chmod
author: pauln
type: progress
---

<pre class='code'>
(pauln@born-of-fire:pauln)$ (cd /s2/pjd-fstest-20080816/ && sudo prove -f tests/chmod/00.t)
tests/chmod/00.t .. 46/58 
not ok 56
tests/chmod/00.t .. Failed 1/58 subtests 

Test Summary Report
-------------------
tests/chmod/00.t (Wstat: 0 Tests: 58 Failed: 1)
  Failed test:  56
Files=1, Tests=58,  7 wallclock secs ( 0.05 usr  0.00 sys +  1.73 cusr  0.11 csys =  1.89 CPU)
Result: FAIL

===============
# POSIX: If the calling process does not have appropriate privileges, and if                                                            
# the group ID of the file does not match the effective group ID or one of the                                                          
# supplementary group IDs and if the file is a regular file, bit S_ISGID                                                                
# (set-group-ID on execution) in the file's mode shall be cleared upon                                                                  
# successful return from chmod().                                                                                                       

expect 0 create ${n0} 0755
expect 0 chown ${n0} 65535 65535
expect 0 -u 65535 -g 65535 chmod ${n0} 02755
expect 02755 stat ${n0} mode
expect 0 -u 65535 -g 65535 chmod ${n0} 0755
expect 0755 stat ${n0} mode

# Unfortunately FreeBSD doesn't clear set-gid bit, but returns EPERM instead.                                                           
case "${os}" in
FreeBSD)
        expect EPERM -u 65535 -g 65534 chmod ${n0} 02755
	expect 0755 stat ${n0} mode
	;;
*)
	expect 0 -u 65535 -g 65534 chmod ${n0} 02755
	expect 0755 stat ${n0} mode
	;;
</pre>
Test 56 is the last one, where we try to set the group sticky bit when the effective gid does not match that of 
the file.
