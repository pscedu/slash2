---
layout: post
title: More chmod permissions bugs
author: pauln
type: progress
---

<pre class='code'>
# this should fail.. but the last chmod succeeds even though there are 
#  no execute permissions on the directory.
(pauln@born-of-fire:pauln)$ mkdir c
(pauln@born-of-fire:pauln)$ touch c/d
(pauln@born-of-fire:pauln)$ chmod 644 c
(pauln@born-of-fire:pauln)$ chmod 640 c/d

# however, if a few seconds go by..
(pauln@born-of-fire:pauln)$ chmod 640 c/d
chmod: cannot access `c/d': Permission denied
</pre>
