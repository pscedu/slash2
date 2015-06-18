---
layout: post
title: Network failure and recovery
author: yanovich
type: progress
---

Over the past few weeks a number of patches addressing reliability in network failures have made their way into the tree.  There are still lurking issues as the patches touch quite involved code but progress is coming along and the functionality should be in standing order "soon".

The implementation makes a number of assumptions which may eventually evolve into tunable knobs.  For example, by default, 10 retries are made before returning error to the issuing client process' I/O syscall.  This hard limit was chosen to prevent indefinite lockups but, as explained, can be easily made tunable.

There are other planned ideas, such as knobs that can be tuned to alert the process' user of the condition (via system mail) or even writing to the stderr of the process (obviously not desired by default).  As well as mechanisms which may allow dynamic control via process environment. The framework should be able to support these endeavors as soon as users request such mechanisms.

In the meantime, the goal is to make unreliable networks more reliable as one of our test environments by chance happens to have very flaky TCP sockets.
