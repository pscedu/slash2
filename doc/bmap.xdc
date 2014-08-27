<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Bmap metadata and user data CRCs</title>
	<taglist>
		<tag>crc</tag>
		<tag>management</tag>
		<tag>calculation</tag>
		<tag>communication</tag>
		<tag>storage</tag>
		<tag>retrieval</tag>
		<tag>generation</tag>
	</taglist>

	<oof:header size="1">Bmap metadata and user data CRCs</oof:header>

	<oof:header size="2">Bmap generations</oof:header>
	<oof:p>
		A bmap has its generation number bumped:
	</oof:p>
	<oof:list>
		<oof:list-item>
			<oof:p>
				when it receives a CRC update from an ION and has at least one
				other <oof:tt>VALID</oof:tt> state in its replica table.
				The other replicas made obsolete by the update are marked as
				<oof:tt>GARBAGE</oof:tt> for reclamation.
			</oof:p>
			<oof:p>
				It should be noted that only one ION may send CRC updates for a
				bmap.
				This ION is serially chosen by the MDS, so long as this bmap
				&harr; ION association is in place, no other IONs may issue CRC
				updates to the MDS.
			</oof:p>
		</oof:list-item>
		<oof:list-item>
			<oof:p>
				all bmaps past the ptrunc position during partial truncation
				resolution.
			</oof:p>
		</oof:list-item>
	</oof:list>
	<oof:p>
	</oof:p>
<!--
	<oof:p>
		At this time I feel that decisions pertaining to bandwidth
		(disk/network) versus metadata capacity are the most critical.
	</oof:p>
-->

	<oof:header size="2">Bmap management</oof:header>
	<oof:p>
		When a client issues a read or write command, an RPC to the MDS is
		made requesting the bmap for the associated file region.
		This document describes the metadata processes involved when
		handling a bmap request from the client.
	</oof:p>

	<oof:header size="3">Bmap Cache Lookup</oof:header>
	<oof:p>
		Note: only on read or write should the bmap tracking incur a log
		operation.
		Otherwise, operations such as <oof:tt>touch(1)</oof:tt> will cause
		an unnecessary log operation.
	</oof:p>
	<oof:p>
		Upon receipt of a <oof:tt>GETBMAP</oof:tt> request, the MDS
		issues a lease to the client used for authenticating I/O activity..
		The bmap cache lookup consists of searching the bmap tree attached
		to each open fcmh for the bmap specified (by numerical ID) in the
		request.
	</oof:p>
	<oof:p>
		If the same client is rerequesting a lease for the same bmap,
		a "duplicate" lease is issued; this is necessary in the protocol for
		situations when the client loses contact with the MDS but the MDS
		hasn't discovered this situation until the reissued request comes
		in.
	</oof:p>
	<oof:p>
		If the bmap already has leases (read or write) to other clients,
		the bmap is degraded into "direct I/O" mode where all clients
		accessing the bmap are forced to perform all I/O without local
		caching to maintain coherency.
	</oof:p>

	<oof:header size="3">Persistent bmap leases</oof:header>
	<oof:p>
		Upon lease issuance, an entry is stored in the MDS persistent
		operations table signifying the lease to rebuild in recovery
		scenarios.
		During recovery (i.e. after failure), these logs are replayed
		to recreate the MDS's cache.
	</oof:p>

	<oof:p>
		While the bmap is being paged in (if it is not already present in the
		MDS memory cache), a placeholder is allocated to prevent reentrant
		page-ins and any additional requesting clients are placed on the
		<oof:tt>bcm_waitq</oof:tt> until the bmap has been loaded.
	</oof:p>

	<oof:p>
		bmaps are fixed size structures.
		To read a specific bmap from an inode's metafile requires the bmap
		index.
		The bmap number multiplied by the bmap ondisk size gives the offset
		into the metafile.
		At this time it should be noted that the metafile file size does not
		accurately reflect the number of bmaps stored in the metafile.
		The file size is used to record the slash2 file's actual size (this
		is done for performance reasons).
		With that in mind, it is possible and likely that bmaps will be read
		from regions of the metafile which have not yet been written.
		In fact, this will happen each time a file is written to for the
		first time.
		So there must be a way to detect when a read bmap is just an array
		of nulls (i.e. bytes read from a non-existent part of the file).
		The null ondisk bmap is not initialized until it must store some
		real data.
		At the time when an ION processes a write for this bmap and sends
		the MDS the bmap's crcs, the MDS is then required to store an
		initialized bmap at the respective index.
	</oof:p>

	<oof:header size="3">Bmap metadata change logging</oof:header>
	<oof:p>
		All modifications to directory inodes, file inodes, and bmaps are
		recorded in a transaction log for replay and MDS replication
		purposes.
	</oof:p>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		In normal read-mode, the bmap is retrieved to give the list of IOS
		replicas (client) and the CRC table (ION).
		The bmap has its own CRC which protects the CRC table and
		replication table against corruption.
	</oof:p>

	<oof:header size="1">File pointer extended past file size</oof:header>
	<oof:p>
		In this case we must create a new bmap on-the-fly with the CRC table
		containing SL_NULL_CRCs (i.e. the CRC of a sliver filled with all null
		bytes).
	</oof:p>

	<oof:header size="1">Passing hole information to the client</oof:header>
	<oof:p>
		Since the client does not have access to the CRC table, the MDS may
		communicate hole information by first checking to see if the crc for
		a given chunk is equal to <oof:tt>SL_NULL_CRC</oof:tt>.
		If it is then the MDS will set a bit to 0.
		These bits will be put to the client in the <oof:tt>GETBMAP</oof:tt>
		reply and will enable the client to know which chunks of the bmap
		must be retrieved from the ION.
	</oof:p>

	<oof:header size="1">Bmap updates</oof:header>
	<oof:p>Bmap metadata is updated and rewritten as a result of numerous
		operations:</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Receipt of a chunk CRC update causes two writes: the store of the
			chunk crc into its appropriate slot and the recomputing and
			rewriting of the bmap CRC.
		<oof:list-item>
		</oof:list-item>
			Replica management: upon successful replication of the bmap or
			when replicas become invalid because of an overwrite.
			This also causes two writes (rewriting of the bmap CRC).
		</oof:list-item>
	</oof:list>

	<oof:header size="2">CRC storage</oof:header>
	<oof:p>
		The dominant issue with CRCs revolves around the data size
		encompassed by a single 8-byte CRC.
		This has direct ramifications in the amount of buffering required
		and the MDS capacity needed to store CRCs.
		Also, since the MDS stores the CRCs, the system ingest bandwidth is
		essentially limited to the number of CRCs the MDS can process.
		Issues regarding the synchronous storing of MDS-side CRCs need to be
		explored.
		For our purposes we will assume that the MDS has safely stored the
		CRCs before acknowledging back to the IOS.
	</oof:p>

	<oof:header size="3">Capacity</oof:header>
	<oof:p>
		The MDS has a fixed size array for CRC storage; the array size is
		the product of the CRC granularity and the bmap size.
		For now we assume that the bmap size is 128MB and the granularity is
		1MB, resluting in an array size of 1K required for CRC storage per
		bmap.
		Here we can see that 8 bytes per 1MB provides a reasonable growth
		path for CRC storage:
	</oof:p>
	<oof:pre>
(1024^2/(1024^2))*8 = 8			# 1MB requires 8B  of CRCs
(1024^3/(1024^2))*8 = 8192		# 1GB requires 8KB of CRCs
(1024^4/(1024^2))*8 = 8388608		# 1TB requires 8MB of CRCs
(1024^5/(1024^2))*8 = 8589934592	# 1PB requires 8GB of CRCs
(1024^6/(1024^2))*8 = 8796093022208	# 1EB requires 8TB of CRCs
</oof:pre>

	<oof:header size="3">Communication from IOS</oof:header>
	<oof:p>
		As writes are processed by the IOS we must ensure that the CRCs are
		accurate and take into account any cache coherency issues that may
		arise.
		One problem we face with parallel IOSes and CRCs is that we have no
		way to guarantee which IOS wrote last and therefore which CRCs
		accurately reflect the state of the file.
		Therefore, revising the parallel IOS write protocol, the MDS will
		determine the IOS &harr; bmap association and provide the same IOS to
		all clients for a given bmap write session.
		This will ensure that only one source is valid for issuing CRC
		updates into a bmap region, and that this source is verifiable by
		the MDS.
		Some failure ramifications:
	</oof:p>
	<oof:p class="padding: 1em">
		Should the write occur but return with a failure, the IOS must have
		a way of notifying the MDS that the CRC state on-disk is unknown.
	</oof:p>
	<oof:p>
		The IOS performs a write and then fails before sending the CRC
		update.
		The CRC should be calculated and stored before writing or sending to
		the MDS.
	</oof:p>
	<oof:p>
		Synchronously delivered update RPCs will surely slow down the write
		process.
		Perhaps we should be able to batch an entire bmap's worth of
		updates.
		Also we should have a journal of client-side CRCs where possible to
		deal with failures, so that for any unsent CRCs (post-failure) may
		be verified the buffer-side crcs against the on-disk state and then
		update the MDS.
	</oof:p>
	<oof:p>
		Need to consider what happens when an IOS fails from the perspective
		of the client and the MDS.
		The MDS may have to log/record bmap &harr; IOS associations to
		protect against updates from a previous IOS ownership.
	</oof:p>

	<oof:header size="2">Design fallouts:</oof:header>
	<oof:p>
		MDS chooses IOS for a given bmap, CRC updates only come from that
		IOS.
	</oof:p>
	<oof:p>
		This means that we can bulk crc updates up to the size of the bmap
		(big performance win).
	</oof:p>
	<oof:p>
		Journal buffer-side CRCs (pre-write) to guard against IOS failure.
		(perhaps not..)
	</oof:p>
	<oof:p>
		MDS RPC to IOS for calculating an entire bmap's worth of CRCs - this
		would be issued when an MDS detects the failure of an IOS and needs
		to reassign.
	</oof:p>
	<oof:p>
		When an MDS chooses an ION for write, he should notify other read
		clients of this.
	</oof:p>


</xdc>
