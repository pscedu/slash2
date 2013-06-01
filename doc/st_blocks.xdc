<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Disk usage accounting</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		This document describes the implementation of support for the
		<oof:tt>struct stat</oof:tt> member <oof:tt>st_blocks</oof:tt>
		of a file.
	</oof:p>
	<oof:p>
		This design does not support the
		<oof:tt>cluster_noshare_lfs</oof:tt> I/O system resource type.
	</oof:p>
	<oof:p>
		When CRC updates are sent from the I/O server,
		<ref sect='8'>sliod</ref> tacks the value of the stat(2) member
		<oof:tt>st_blocks</oof:tt> for the file into the RPC.
	</oof:p>
	<oof:p>
		When received by the MDS, the MDS takes the following actions:
	</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			the new file size sent by the I/O server is checked if it extends
			the current file size as known to the MDS.
		</oof:list-item>
		<oof:list-item>
			the index for the IOS sending the update is looked up in the
			file's replication table.
		</oof:list-item>
		<oof:list-item>
			a delta between the current
			<oof:tt>ino_repl_nblks[iosidx]</oof:tt> value and the new value as
			sent by <ref sect='8'>sliod</ref> is calculated and this
			difference is added to the <oof:tt>sst_blocks</oof:tt> value.
		</oof:list-item>
		<oof:list-item>
			at this time, the <oof:tt>fcmh_sstb</oof:tt> are written via
			SETATTR into the metadata backend file system without journal
			logging.
		</oof:list-item>
		<oof:list-item>
			the <oof:tt>ino_repl_nblks[iosidx]</oof:tt> value is updated and
			the inode/inox is written via WRITE into the metadata backend file
			system without journal logging.
		</oof:list-item>
		<oof:list-item>
			the bmap CRCs are updated to the new values specified in the RPC
			and a journal entry is written containing:
			<oof:list>
				<oof:list-item>these CRC values</oof:list-item>
				<oof:list-item>the new file size</oof:list-item>
				<oof:list-item>whether the file size constituted an
					extending</oof:list-item>
				<oof:list-item>the IOS replica table index</oof:list-item>
				<oof:list-item>the new <oof:tt>ino_repl_nblks[iosidx]</oof:tt>
					value</oof:list-item>
				<oof:list-item>the new aggregate <oof:tt>sst_blocks</oof:tt>
					value.</oof:list-item>
			</oof:list>
		</oof:list-item>
	</oof:list>
	<oof:p>
		On replay, these same steps are (essentially) taken to recover any
		loss.
	</oof:p>
</xdc>
