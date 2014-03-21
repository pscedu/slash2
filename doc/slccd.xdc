<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Caching Exploration</title>

	<oof:header size="1">Caching Exploration</oof:header>

	<oof:p>
		This document describes the architecture of a cache-coherent
		distributed read-only I/O daemon for use by clients in a SLASH2
		deployment.
		This effectively provides the cooperative caching model suggested by
		deployments that would benefit from such a capability.
	</oof:p>

	<oof:p>
		slccd (the SLASH2 cache coherent daemon) is configured to use some
		amount of disk store to capture read-only data.
		Old data is deleted as space is needed for new data entering the
		cache as determined by a "last use" field either in memory or a
		metadata file stored locally.
	</oof:p>

	<oof:p>
		When mount_slash handles open(2), if it is configured to use a local
		slccd and the file st_mode is read-only, all read I/O requests are
		sent to slccd instead of the preferred I/O system.  Otherwise, it
		performs the read I/O as normal.
	</oof:p>

	<oof:p>
		The slccd uses the following pieces of information to determine data
		residency for content in his cache:
	</oof:p>

	<oof:list>
		<oof:list-item>file ID (FID)</oof:list-item>
		<oof:list-item>file offset (bmap/sliver offset and length)</oof:list-item>
		<oof:list-item>CRCs for the sliver(s)</oof:list-item>
	</oof:list>

	<oof:p>
		If the data is not present, it is fetched and populated into the
		cache.
	</oof:p>

	<oof:p>
		If the file is no longer marked read-only, all data is eradicated
		from the cache and this failure condition is returned to the client,
		who must perform the read I/O directly.
	</oof:p>

	<oof:p>
		Otherwise, the "last use" field is updated for the data region in
		question so that it stays in the cache and the data is returned to
		the client.
	</oof:p>

	<oof:p>
		There is no need to build "local cache" support directly into
		mount_slash taking this route.
		Local cache is provided by running an instance of slccd on the local
		machine.
	</oof:p>

	<oof:p>
		Is this a better approach?
		Perhaps a better approach would be to merge mount_slash and sliod
		and add the functionality of slccd.
	</oof:p>

</xdc>
