#!/usr/bin/env python
# $Id$

import sys, os
from os.path import join

if len(sys.argv) < 4:
  print "Not enough arguments! rootdir 1 inode1 inode2 ..."
rootdir, count = sys.argv[1], 0
inodes = [int(x) for x in sys.argv[3:]]
for root, dirs, files in os.walk(rootdir):
  for f in files:
    path = join(root, f)
    fd = os.open(path, os.O_RDONLY)
    inode = os.fstat(fd).st_ino
    os.close(fd)
    if inode in inodes:
      print "{} {}".format(inode, path)
      inodes.remove(inode)
      if len(inodes) == 0:
	sys.exit(0)
