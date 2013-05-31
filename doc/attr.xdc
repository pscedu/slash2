<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Attribute (metadata) handling in the MDS</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>Currently SLASH2 has two ways of updating attributes:</oof:p>
	<oof:list>
		<oof:list-item>the client can issue a setattr RPC to mds.</oof:list-item>
		<oof:list-item>the i/o server can issue a crc-update RPC,
			piggybacking size and mtime</oof:list-item>
	</oof:list>
	<oof:p>
		To avoid these two RPCs cross path with each other, we have a
		utimegen mechanism so that an attribute update is only allowed by
		the crc-update path if it has the right generation number
		(utimegen).
		This generation number is stored in the ZFS layer (zp_s2utimgen) and
		is bumped each time we update mtime.
		It only applies to mtime.
	</oof:p>
	<oof:p>
		Using generation number means that the crc-update path has to
		compete to get the right to update the attributes.
		Instead, we should really update attributes based on when the
		corresponding operation (chmod, write, etc) happened on the client.
		Because utimegen only applies to mtime, crc-update path always
		increases file size, which could be a problem as well.
		Imagine that a client writes some data to increase the file size and
		then decides to truncate the file size.
		The final file size could be incorrect.
	</oof:p>
	<oof:p>
		Another issue with the current mechanism is that the most recent
		attributes of a file can be at either mds or ios.
		When the fchm (file cache entry) of a client needs to be
		re-established, where does it get attributes from?
		The most recent attributes might still be en route to mds from IOS.
	</oof:p>
	<oof:p>
		We could make the current scheme work by adding tricks here and
		there.
		But to make things robust, we need a simpler way.
		We will let client be the only one that can update the attributes.
		It should work regardless of network delays and service outage.
	</oof:p>
</xdc>

