<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>MDS bmap management</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		In normal read-mode, the bmap is retrieved to give the list of IOS
		replicas (client) and the CRC table (ION).
		The bmap has its own CRC (bmap_crc) which protects the CRC table and
		replication table against corruption.
	</oof:p>

	<oof:header size="1">File pointer extended past file size</oof:header>
	<oof:p>
		In this case we must create a new bmap on-the-fly with the crc table
		containing SL_NULL_CRC (the CRC of all nulls).
	</oof:p>

	<oof:header size="1">Passing hole information to the client</oof:header>
	<oof:p>
		Since the client does not have access to the crc table, the MDS may
		communicate hole information by first checking to see if the crc for
		a given chunk is == SL_NULL_CRC.
		If it is then the MDS will set a bit to '0'.
		These bits will be put to the client in the SRMT_GETBMAP reply and
		will enable the client to know which chunks of the bmap must be
		retrieved from the ION.
	</oof:p>

	<oof:header size="1">Bmap updates</oof:header>
	<oof:p>Bmaps must be updated at several points:</oof:p>
	<oof:list type="LIST_UN">
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
