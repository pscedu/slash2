<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>High-Level Drop Operations</title>

	MDS drops Client
	<oof:p>
		MDS cleans entire client export.
		At the moment this is performed through slashrpc_export_destroy()
		which calls mexpfcm_cfd_free() on every open file.
		mexpfcm_cfd_free() removes a single reference from each bmap
		attached to the fidc_membh.
	</oof:p>
	<oof:p>
		Note that no logging occurs here.
		It was thought previously that we would need to journal all fcmh
		opens, which would imply that their closing operations would also
		require a journal operation.
		However this is no longer the case since fcmh opens are verified by
		key-signed fdbuf's which are verifiable across reboots.
		Further, cold-cache opens may take place via the immutable
		namespace.
	<oof:p>

	MDS drops ION
	<oof:p>
		Failed or disconnected IONs pose a trickier problem than unavailable
		clients because of the journaling operations that result and the
		notification of clients.
		First we must iterate across the export's set of bmaps, these are
		the bmaps which the MDS has assigned to the ION for writing.
		These bmaps must be closed out in the log and marked as 'unknown',
		signifying their crc table's are possibly compromised.
		Additionally, these bmaps are scheduled to be re-read so that the
		crc's may be stored on the MDS.
	</oof:p>
	<oof:p>
		Finally, the clients of these bmaps (which are all write-bmaps) are
		notified to relinquish their set of cached bmaps which correspond to
		the failed ION.
	</oof:p>
	Steps involved:
		Close bmap associations in the journal and mark them as 'unknown'.
		Schedule bmap's to be re-read.
		Notify clients to release their bmaps which point to the failed ION.
	<oof:p>
		Should the MDS fail during these operations the system should not be
		compromised.
		On reboot the MDS will scan its bmap <--> ION association log to
		determine the state of the bmaps.
		Bmaps belonging to the failed ION which were not closed prior to MDS
		failure will still be considered 'open' but marked as 'unknown'.
		In fact any 'open' bmap found in the log will be marked as
		'unknown'.
		The handling of 'unknown' bmaps causes them to be scheduled for
		re-reading, so again, we should not miss any operations if the MDS
		fails.
		Clients will have their own logic for handling ION failures, MDS
		notifications are really just 'helpful suggestions'.
	</oof:p>

	ION drops Client
	<oof:p>
		Similar to the MDS dropping a client.
		The bmaps attached to the failed client's export are iterated,
		decref'd, and possibly freed.
		The final step is to cleanup the export.
	</oof:p>

	ION drops MDS
	<oof:p>
		The ION's have no log and hence operate purely from memory.
		When an MDS fails the ION tries to maintain its state and wait for
		the MDS to return.
		The MDS's bmap association log informs the MDS of the IONs which
		have write permission to specific bmaps.
		In the case of an MDS failure, the ION essentially waits around for
		it to return.
		So long as the ION has available memory for queued operations to the
		MDS, it may still accept write operations from the clients.
		Read operations may always be handled so long as all parties are
		satisfied with the state of their read-bmaps (ie it has not timed
		out).
	</oof:p>
	<oof:p>
		CRC update RPCs should be accompanied by transaction numbers so that
		the MDS and ION can determine the state of the CRC tables without
		issuing a re-read operation.
		The transaction number would be stored in the main journal on the
		MDS with each crcupdate operation.
		The ION would keep the transaction ID in his MDS import.
		Upon reconnection the MDS will have restored his export structure
		from the log and the transaction ID.
		Should the transaction IDs match, no re-read is necessary.
		However if the transaction IDs do not match then we may have to
		assume that the connection has been compromised (i.e. perhaps the ION
		rebooted too).
	</oof:p>

	Client drops ION
	<oof:p>
		Client must iterate across the ION's import and redirect read-bmaps
		to alternate destinations which are either another ION in the same
		IOS or a replica on another IOS.
		Write-enabled bmaps must be re-requested from the MDS.
		Note that the client may still have cached buffers attached to that
		bmap.
	</oof:p>

	Client drops MDS
	<oof:p>
		In this case nothing is cleaned or re-requested.
		Open file handles remain valid so long as the bdbuf is intact.
	</oof:p>
</xdc>
