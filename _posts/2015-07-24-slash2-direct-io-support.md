---
layout: post
title: io_submit() support for SLASH2
author: zhihui
type: progress
---

Direct I/O support has been in the slash2 for a long time.  It is
done through the direct_io flag set on a per-file basis.

On the other hand, the Linux kernel does not have O_DIRECT support
for a FUSE file system until kernel release 3.4. Later, in release 
3.10, the Linux kernel added asynchronous direct I/O support for FUSE.
This is important for applications like mySQL that does things like
io_submit().

However, mySQL fails with EINVAL on slash2. We tried every release
from 3.10 all the way to 4.1 release.  As it turns out, the two
ways to specify direct I/O (O_DIRECT versus direct_io) are not 
well-integrated until Linux 4.1 release.  For earlier releases, we have 
to use the following workaround to make aynchronous direct I/O work on slash2:

<pre>
        # msctl -p sys.direct_io=0
</pre>

Note that even if slash2 turns off its side of direct I/O, an application
can continue to reap the benefits of direct I/O if they open a file with O_DIRECT.

For a curious mind, I believe that the following kernel patch fixes
the problem:

<pre>

zhihui@krakatoa:~/linux-git$ git show 15316263649d9eed393d75095b156781a877eb06 | head -8
commit 15316263649d9eed393d75095b156781a877eb06
Author: Al Viro <viro@zeniv.linux.org.uk>
Date:   Mon Mar 30 22:08:36 2015 -0400

    fuse: switch fuse_direct_io_file_operations to ->{read,write}_iter()
    
    Signed-off-by: Al Viro <viro@zeniv.linux.org.uk>

</pre>
