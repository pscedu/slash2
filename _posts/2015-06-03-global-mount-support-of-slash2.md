---
layout: post
title: Global mount support for SLASH2
author: zhihui
type: progress
---

Global mount is currently the SLASH2's way to support distributed metadata server.
It uses a few bits in the FID space to determine the MDS that a client should talk to in order to access the corresponding file.

SLASH2 has a configuration file that can be used to find all the MDSes without the need to have another higher-level manager.
When a SLASH2 instance is mounted on a client, it sends the ROOT pid 1 to a metadata server.
Depending on whether global mount is enabled on the target MDS or not, it can return either the super root or the root of the name space of the target MDS. 

Here is some ASCII art:

<pre>
                          +-----+
                          |  /  |      super root
                          +-----+
                             |
                             |
         +-------------------+-------------------+
         |                   |                   |
         v                   v                   v
      +-----+            +------+             +-----+
      | PSC |            | PITT |             | CMU |
      +-----+            +------+             +-----+
         |                   |                   |
         |                   |                   |
    +---------+         +---------+         +---------+
    |         |         |         |         |         |
    v         v         v         v         v         v
+-------+ +-------+ +-------+ +-------+ +-------+ +-------+
| dir1/ | | file1 | | dir2/ | | file2 | | dir3/ | | file3 |
+-------+ +-------+ +-------+ +-------+ +-------+ +-------+
</pre>
For example, if we mount against the MDS at PSC, we will see dir1 and file1 under the root.
If global mount is enabled on PSC, then we will see PSC, PITT,  CMU under the root.

Afterward the client is responsible for contacting the correct MDS for leases.

As expected, hardlinks across two MDSes will be rejected.
Symbolic links work, but it is advisable to use absolute target names.
This is consistent with regular file systems.

In addition, no regular files and directories can be created under the super root.

