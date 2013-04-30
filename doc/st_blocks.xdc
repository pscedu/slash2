<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		This document describes the implementation of 'struct
		stat' st_blocks support in the SLASH2 file system.
	</oof:p>
	<oof:p>
		This design does not support the 'cluster_noshared' I/O server
		resource type.
	</oof:p>
	<oof:p>
		When CRC updates are sent from the I/O server, sliod tacks the value
		of stat(2) st_blocks for the file into the RPC.
	</oof:p>
	<oof:p>
		When received by the MDS, the MDS takes the following actions:
	</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			the new file size sent by the I/O server is checked if it extends
			the current file system as known to the MDS.
		</oof:list-item>
		<oof:list-item>
			iosidx is looked up in the file's replication table for the sliod
			sending the update.
		</oof:list-item>
		<oof:list-item>
			a delta between the current ino_repl_nblks[iosidx] value and the
			new one as sent by the sliod is calculated and this difference is
			applied to the sst_blocks value.
		</oof:list-item>
		<oof:list-item>
			at this time, the fcmh_sstb are written via SETATTR into the ZFS
			meta backend file system without journal logging.
		</oof:list-item>
		<oof:list-item>
			the ino_repl_nblks[iosidx] value is updated and the inode/inox is
			written via WRITE into the ZFS meta backend file system without
			journal logging.
		</oof:list-item>
		<oof:list-item>
			the bmap CRCs are updated to the new values specified in the RPC
			and a journal entry is written containing these CRC values, the
			new file size, whether the file size constituted an extending, the
			iosidx value, the new ino_repl_nblks[iosidx] value, and the new
			aggregate sst_blocks value.
		</oof:list-item>
	</oof:list>
	<oof:p>
		On replay, these same steps are (essentially) taken to recover any
		loss.
	</oof:p>
</xdc>
