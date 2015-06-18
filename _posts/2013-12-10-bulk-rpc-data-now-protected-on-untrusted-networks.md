---
layout: post
title: Bulk RPC data now protected on untrusted networks
author: yanovich
type: progress
---

Some changes to the RPC layer in SLASH2 have been made recently that provides cryptographic protection over bulk data sent via RPCs among SLASH2 nodes.  Previously, only message headers were protected.  This really only provides message integrity (and not confidentiality) and at the cost of the computational overhead which essentially digitally signs the data but it is not consistent with the way SLASH2 handles all other network traffic with peers.  Some bulk data was already protected with specific handling to make it so but now all is, so network bit flips, etc. should now be largely avoided thanks to the cryptographic routines.

More ideas are on the way about how to have untrusted clients (non root) on a SLASH2 network in a non-disruptive manner, how to split SLASH2 nodes across internal networks, and how to alleviate performance overhead on fast trusted networks...
