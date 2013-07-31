<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

This document describes the architecture of a cache-coherent distributed
read-only I/O daemon for use by clients in a SLASH2 deployment.  This
effectively provides the cooperative caching model suggested by
deployments that would benefit from such a capability.

The slccd (SLASH2 cache coherent daemon) is configured to use some
amount of disk store to capture read-only data.  Old data is deleted as
space is needed for new data entering the cache as determined by a "last
use" field either in memory or a metadata file stored locally.

When mount_slash handles open(2), if it is configured to use a local
slccd and the file st_mode is read-only, all read I/O requests are sent
to slccd instead of the preferred I/O system.  Otherwise, it performs
the read I/O as normal.

The slccd uses the following pieces of information to determine data
residency for content in his cache:

    (o) file ID (FID)
    (o) file offset (bmap/sliver offset and length)
    (o) CRCs for the sliver(s)

If the data is not present, it is fetched and populated into the cache.

If the file is no longer marked read-only, all data is eradicated from
the cache and this failure condition is returned to the client, who must
perform the read I/O directly.

Otherwise, the "last use" field is updated for the data region in
question so that it stays in the cache and the data is returned to the
client.

There is no need to build "local cache" support directly into
mount_slash taking this route.  Local cache is provided by running an
instance of slccd on the local machine.
