---
layout: post
title: zfs build changes
author: yanovich
type: progress
---

Yesterday, I revised the way we build ZFS which we use in the MDS backend for the file system metadata storage.

Previously, there was a <tt>make(1)</tt> target <tt>zbuild</tt> that branded two compilations of the ZFS codes: one that built the standalone executables such as <tt>zpool(8)</tt> and <tt>zfs-fuse(8)</tt> and the second which constructed a library providing the core ZFS functionality for use in <tt>slashd</tt> which we call <tt>libzfs-fuse.a</tt>.

The problem was that, anytime hacking on ZFS occurs, because both compilations reused object file naming, <em>you had to clear out both compilations and build both over again from scratch</em>.  So I eliminated the <tt>zbuild</tt> target in place of our common <tt>make</tt> infrastructure targets.  Now, to build ZFS, simply descend into <tt>${ROOTDIR}/zfs</tt> and run <tt>make</tt> there.

The new build process maintains separate object directories for the standalone utilities and for <tt>libzfs-fuse.a</tt> which minimizes the amount of work to be done when source files shared between them get modified.  This approach also has the benefit of reducing surprises and unique make targets to bootstrap the system codes by following the general procedure for building our components.
