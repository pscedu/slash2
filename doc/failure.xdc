<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>High-Level connection dropping operations</title>

	<oof:header size="1">MDS: client gets dropped</oof:header>
	<oof:p>
		No special action.
	</oof:p>

	<oof:header size="1">MDS: ION gets dropped</oof:header>
	<oof:p>
		The update scheduler is notified to revert any inflight operations
		back to their queued state as no state updates can happen when I/O
		servers are offline (from the MDS's perspective).
		Flags and bandwidth reservations indicating any such arrangments
		e.g. via replication are relinquished.
	</oof:p>
	<oof:p>
		As bmap &amp;#8596; ION lease assignments (bia) are tracked persistently,
		the MDS will recover any such lost state on startup.
	</oof:p>

	<oof:header size="1">MDS: MDS gets dropped</oof:header>
	<oof:p>
		No special action.
	</oof:p>

	<oof:hr />

	<oof:header size="1">ION: MDS gets dropped</oof:header>
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

	<oof:header size="1">ION: ION gets dropped</oof:header>
	<oof:p>
		todo
	</oof:p>

	<oof:header size="1">ION: CLI gets dropped</oof:header>
	<oof:p>
		Similar to the MDS dropping a client.
		The bmaps attached to the failed client's export are iterated,
		decref'd, and possibly freed.
		The final step is to cleanup the export.
	</oof:p>

	<oof:hr />

	<oof:header size="1">CLI: MDS gets dropped</oof:header>
	<oof:p>
		Replication status inquiries are all given up on.
	</oof:p>

	<oof:header size="1">CLI: ION gets dropped</oof:header>
	<oof:p>
		Asynchronous I/O requests are all given up on.
	</oof:p>
	<oof:p>
		Before all I/O activity, online availability of IONs is
		factoring into selection of an IOS.
		For situations when no IONs are available (who harbor residency of
		the requested data, exclusively), the client will either block
		awaiting connectivity or fail instantly, depending on configuration.
	</oof:p>
	<oof:p>
		Any failures during synchronous I/O activity are handled directly
		and not in any callback fashion.
	</oof:p>
	<oof:p>
		Note that the client may still have cached buffers attached to
		bmaps leased associated with such IONs.
		The client will retain and retry until success or bail and purge his
		cache, depending on configuration, as long as the MDS respects his
		ability to do so, which would only happen if communication with the
		MDS (from the MDS' perspective) is lost as well.
	</oof:p>

</xdc>
