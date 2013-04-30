<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Binding bmaps to I/O servers</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		The metadata server must bind bmaps to specific IO nodes in order
		guaranteeing the correctness of the bmap's crc table (which is
		stored on the MDS).
		At the moment, the most straight forward way for tracking and
		verifying these bindings is to log them on the metadata server.
		This ensures that in the event of a crash the MDS can reliably
		determine the existing bmap &harr; ION relationships.
	</oof:p>
	<oof:p>
		The processes described here primarily apply to write-enabled bmaps.
		Read-only bmaps are only tracked in memory for cache coherency
		purposes and are only managed when the mode of the bmap is changed
		from read-only to read-write.
		In this case the clients are routed to the same ION and instructed
		to use direct I/O.
	</oof:p>

	<oof:header size="1">Bmap Write Assignment Procedure</oof:header>
	<oof:p>
		For now we choose a protocol which requires the client to ask the
		MDS for a write bmap.
		Upon receiving a bmap-write request, the MDS does the following:
	</oof:p>

	<oof:list type="LIST_UN">
		<oof:list-item>
			Determines if the bmap is already active
		</oof:list-item>
		<oof:list-item>
			If the bmap is already open for writing then an ION has already
			been chosen and its identifier is returned to the client.
		</oof:list-item>
		<oof:list-item>
			Open for read, then the most appropriate ION must be chosen based
			on the location of the readers.
		</oof:list-item>
		<oof:list-item>
			In any case where multiple writers exist, or several readers and a
			writer are present, the clients and ION must work in directio mode
			to preserve coherency.
		</oof:list-item>
		<oof:list-item>
			For inactive bmaps, the MDS sends a request to the chosen ION
			(based on the client's IOS preference) which contains a key for
			write access.
		</oof:list-item>
		<oof:list-item>
			Upon successful reply, the MDS returns the ION number and key to
			the client.
		</oof:list-item>
		<oof:list-item>
			At this point, the client is permitted to issue writes to the ION
			for the given bmap and.
		</oof:list-item>
		<oof:list-item>
			The ION is now permitted to issue CRC updates to the MDS for this
			bmap.
		</oof:list-item>
	</oof:list>

	<oof:p>
		This protocol has one disadvantage.
		The latency for acquiring a write bmap is relatively high because
		the MDS must contact the ION before replying to the client.
		Users writing large sequential files may notice performance drops
		when crossing bmap boundaries.
		However, the approach is very straight forward and the design should
		not disallow future performance improvements.
	</oof:p>
	<oof:p>
		One such improvement may be to allow a client and ION to have write
		access to any bmap in the file without having to explicitly request
		each bmap.
		Such a method would only be applicable to newly created or truncated
		files.
		Bmaps would be created at the MDS on-the-fly as the ION issued crc
		updates for the various offsets it was handling.
	</oof:p>
	<oof:p>
		To ensure proper recovery after a crash, the MDS must maintain a log
		of its bmap &lt;-&gt; ION bindings.
		Since these assignments may be valid for extended periods of time it
		seems impractical to write them into the MDS's primary log.
		Instead we intend to log these assignments in a separate log file to
		minimize the effects of log wrapping.
		In the event where an active assignment is going to be overwritten
		by a log wrap, the assignment will be refreshed in place.
	</oof:p>
	<oof:p>
		The ION tries to minimize the effects of long-standing bmap
		assignments by closing bmaps which are not active.
		This allows the MDS to release the state associated with the
		assignment by closing it in the journal and releasing the memory
		objects resident in the export.
		In order to maintain the integrity of the bmap CRC tables it is
		vital that the ION initiates the bmap closing process.
		It would be possible for the MDS to suggest which bmaps be closed
		but this approach will not be pursued at this time.
	</oof:p>

	<oof:header size="1">Bmap Closing Procedure</oof:header>
	<oof:p>
		When an ION decides to close out a bmap it sends a notification to
		the MDS and then to any clients which were accessing it.
		The clients can be found through the tree of exports attached to the
		bmap.
	</oof:p>
	<oof:p>
		Clients which issue a write for a closed bmap will be issued an
		error and forced to contact the MDS to renew the bmap.
	</oof:p>

	<oof:header size="1">ION Failure Handling</oof:header>
	<oof:p>
		Most ION service failures should be handled (both clients and mds)
		by issuing a high-level drop callback through LNET.
		LNET issues this callback upon detection of a failed socket.
		The HLD issues a callback specific to the import or export attached
		to the connection.
		Through this mechanism we intend to release the bmaps attached to
		either the import (client) or the export (MDS).
	</oof:p>

	<oof:header size="1">ION Failure Handling by MDS</oof:header>
	<oof:p>
		MDS-side handling of ION failures entails the release of all bmaps
		on the ION's respective export.
		All bmaps on the export have their CRC states marked as 'unknown'.
		These bmaps will be scheduled to have their CRCs recalculated by
		another ION in the IOS or the failed ION upon restart.
		The design details of this process are still murky but it seems that
		they will be scheduled in a manner similar to that of a bmap
		replication request.
		What happens to modifications to the bmap during both CRC
		re-verification or replication is still unknown.
		It is likely that the bmap will remain read-only during these
		processes.
		The size of the bmap is small enough that neither a CRC verification
		or replication operation should take more than a few seconds.
	</oof:p>
	<oof:p>
		In addition to scheduling CRC recalculations, the MDS will notify
		all clients attached to the bmaps to revoke them from their caches.
	</oof:p>
	<oof:p>
		It should be noted that when an ION issues an crcupdate request for
		a bmap which the MDS cannot verify the binding that the MDS must
		mark that bmap's CRC table's state as 'unknown'.
		For performance reasons the ION writes to his local filesystem first
		and then issues the crcup request.
		Therefore the MDS must assume that a write has occurred upon
		receiving a crcup request.
		It should be noted that invalid crcup requests could be a source of
		a nasty DOS attack, therefore SSL enabled sockets will be needed in
		SLASH2 production environments.
	</oof:p>

	<oof:header size="1">ION Failure Handling by Client</oof:header>
	<oof:p>
		Client-side handling of ION failures entails the release of all
		bmaps on the ION's respective import.
		The fallout of which may take on many forms.
		For read bmaps two strategies may be taken, neither of which
		necessarily involve the MDS.
		If the ION's owning system contains multiple IONs then the client
		may redirect his read request to that ION.
		Otherwise the client may access an alternate replica.
		The replica table has already been made available to him via the
		bmap itself.
		If no replicas exist and the IOS only contains the single failed ION
		then the client must block or fail the read.
	</oof:p>
	<oof:p>
		Dealing with write-enabled bmaps is a more complicated issue because
		of MDS involvement and the possible presence of other writers.
		If the MDS has already issued an HLD, or is in the process, then the
		bmap may be reassigned.
		Otherwise the MDS does not agree with the client on the state of the
		ION - it goes without saying that this situation is problematic.
		To handle this case today, the MDS will only reassign the bmap if he
		has issued an HLD for the failed ION.
	</oof:p>
	<oof:p>
		Note: Clients will be required to timeout their own read-only bmaps
		to prevent the situation where they missed a revoke request from the
		MDS.
		This is to ensure that the client is reading from the most recent
		version of the bmap.
		It may be worthwhile to pursue a protocol where a client's read-only
		bmaps are verified in bunches.
	</oof:p>
</xdc>
