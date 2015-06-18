---
layout: post
title: Replication starvation issue resolved
author: yanovich
type: progress
---

A first run in the guts of the SLASH2 metadata server replication engine left an issue with starvation of replication workloads queued up by users.  A classical starvation issue, a single user can inject many work items and deny service to other users as pieces of work are selected at random, instead of randomly throughout the user class.

Student interns Chris Ganas and Tim Becker provided some exploration and improvements in the code to do fair sharing in the engine efficiently and in a manner that can scale to large system workloads.  Good work and thanks guys!
