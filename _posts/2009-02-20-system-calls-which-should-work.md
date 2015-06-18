---
layout: post
title: System calls which should work
author: pauln
type: progress
---

The fuse handlers for these calls are located in <tt>mount_slash/main.c</tt>.

<pre class='code'>
Works  Fs Call       Handler
-----  -----------   ------------
Y      mount()       fuse native.
Y      statfs()      slash2fuse_statfs()
Y      creat()       slash2fuse_create()
Y      open()        slash2fuse_open()
Y      stat()        slash2fuse_getattr()
Y      fstat()       slash2fuse_getattr()
Y      mkdir()       slash2fuse_mkdir()
Y      readdir()     slash2fuse_readdir_helper()
Y      rmdir()       slash2fuse_rmdir_helper()
Y      unlink()      slash2fuse_unlink_helper()
Y      close()       slash2fuse_release() / slash2fuse_releasedir()
?      access()      slash2fuse_access()
N      read()
N      write()
N      readlink()
N      mknod()
N      symlink()
N      fsync()
</pre>
