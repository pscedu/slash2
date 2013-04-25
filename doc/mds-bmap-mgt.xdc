<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>MDS bmap management</title>

	<oof:p>
		In normal read-mode, the bmap is retrieved to give the list of IOS
		replicas (client) and the CRC table (ION).
		The bmap has its own CRC (bmap_crc) which protects the CRC table and
		replication table against corruption.
	</oof:p>

	<oof:h1>File Pointer Extended Past Filesize</oof:h1>
	<oof:p>
		In this case we must create a new bmap on-the-fly with the crc table
		containing SL_NULL_CRC (the CRC of all nulls).
	</oof:p>

	<oof:h1>Passing Hole Information to the Client</oof:h1>
	<oof:p>
		Since the client does not have access to the crc table, the MDS may
		communicate hole information by first checking to see if the crc for
		a given chunk is == SL_NULL_CRC.
		If it is then the MDS will set a bit to '0'.
		These bits will be put to the client in the SRMT_GETBMAP reply and
		will enable the client to know which chunks of the bmap must be
		retrieved from the ION.
	</oof:p>

	<oof:h1>Bmap Updates</oof:h1>
	<oof:p>Bmaps must be updated at several points:</oof:p>
	<oof:list>
		<oof:list-item>
			Receipt of a chunk crc update causes two writes: the store of the
			chunk crc into its appropriate slot and the recomputing and
			rewriting of the bmap_crc.
		<oof:list-item>
		</oof:list-item>
			Replica management: upon successful replication of the bmap or
			when replicas become invalid because of an overwrite.
			This also causes two writes (rewriting of the bmap_crc).
		</oof:list-item>
	</oof:list>
</xdc>
