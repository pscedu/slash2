<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Truncation</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		This document describes the implementation of truncate operations in
		SLASH2.
	</oof:p>
	<oof:p>
		There are two kinds of truncates:
	</oof:p>
	<oof:list type="LIST_UN">
    <oof:list-item>
			full truncate, i.e. truncate to file offset position zero
    </oof:list-item>
    <oof:list-item>
			partial truncate, i.e. truncate to a non-zero file offset position
		</oof:list-item>
	</oof:list>

	<oof:header size='2'>Full truncation</oof:header>
	<oof:p>
		In the MDS, full truncates cause the file generation number to be
		bumped and recording of the old file ID (FID) and file generation
		number (fid+gen) into a logfile which is eventually shipped to all
		sliods that were registered in the file replica table.
		Once these logfiles are successfully received by all sliods they
		pertain to and the sliods reply signifying the actions have been
		applied, these logfiles are deleted.
	</oof:p>
	<oof:p>
		Other actions that evoke this same garbage collection mechanism are:
	</oof:p>

	<oof:list type="LIST_UN">
		<oof:list-item><oof:tt>unlink(2)</oof:tt> syscall</oof:list-item>
		<oof:list-item>a clobbering <oof:tt>rename(2)</oof:tt>; i.e.
			overwriting an existing file</oof:list-item>
	</oof:list>

	<oof:header size='2'>Partial truncation</oof:header>
	<oof:p>
		Partial truncation is caused solely by the
		<oof:tt>truncate(2)</oof:tt> system call.
	</oof:p>
	<oof:p>
		First, if the file is already marked <oof:tt>IN_PTRUNC</oof:tt>,
		signifying that it is already handling partial truncation resolution
		(i.e. the steps outlined here), failure is returned immediately to
		the client issuing the SETATTR and the client is registered on a
		list to be notified of completion after resolution has occurred.
		Only one partial truncation on a file can be happening at any given
		time.
	</oof:p>
	<oof:p>
		Next, the file is marked (in memory only, for now)
		<oof:tt>IN_PTRUNC</oof:tt> and the client is notified that this
		behavior is taking place.
		At this point, it is the client's responsibility to reissue this
		SETATTR operation in case of communication failure as the MDS
		provides no guarentees yet that the operation will be recorded by a
		journal or other persistent behavior tracker.
	</oof:p>
	<oof:p>
		Next, any leases currently granted to clients for bmaps included
		within or falling after the partial truncate file offset position
		(hereby referred to as the <oof:emph>ptrunc position</oof:emph>) are
		instructed to released.
	</oof:p>
	<oof:p>
		The MDS then waits for clients to relinquish all said leases,
		waiting a maximum of the bmap timeout time in the case of
		unresponsive clients.
	</oof:p>
	<oof:p>
		The next action is determined depending on the value of the ptrunc
		position:
	</oof:p>

	<oof:list type="LIST_UN">
		<oof:list-item>
			if the ptrunc position falls cleanly between two bmaps, the
			following actions are taken:

			<oof:list type="LIST_UN">
				<oof:list-item>
					bmaps after the ptrunc position are marked
					<oof:tt>VALID</oof:tt> &rarr; <oof:tt>GARBAGE</oof:tt> and
					written to disk, and journaled;
				</oof:list-item>
				<oof:list-item>
					the new file size is saved in <oof:tt>sst_size</oof:tt> and
					written to disk without journaling; and </oof:list-item>
				<oof:list-item>
					the <oof:tt>IN_PTRUNC</oof:tt> flag is cleared from the file
				</oof:list-item>
			</oof:list>
		</oof:list-item>
		<oof:list-item>
			if the ptrunc position falls within a bmap,
			<ref sect='p'>upsch</ref> work is queued to resolve the CRC
			recalculation for the affected sliver that must occur before
			processing can return to normal.
		</oof:list-item>
	</oof:list>

	<oof:p>
		All full bmaps past the ptrunc position are marked
		<oof:tt>VALID</oof:tt> &rarr; <oof:tt>GARBAGE</oof:tt> and the bmap
		where the ptrunc position lies is marked <oof:tt>VALID</oof:tt>
		&rarr; <oof:tt>TRUNCPNDG</oof:tt>.
	</oof:p>

	<oof:p>
		At earliest convenience (although of higher priority than any
		replication activity), a randomly selected
		<oof:tt>TRUNCPNDG</oof:tt> marked <ref sect='8'>sliod</ref> is asked
		to perform the CRC recalculation.
		When one finally does, the bmap is marked <oof:tt>TRUNCPNDG</oof:tt>
		&rarr; <oof:tt>VALID</oof:tt> and other replicas are marked
		<oof:tt>TRUNCPNDG</oof:tt> &rarr; <oof:tt>GARBAGE</oof:tt>.
	</oof:p>

	<oof:p>
		At this point, the MDS issues <oof:tt>BMAP_WAKE</oof:tt>
		notifications to the original client as well as to any new clients
		that attempted <oof:tt>SETATTR</oof:tt> or
		<oof:tt>BMAP_LEASE</oof:tt> requests since
		<oof:tt>IN_PTRUNC</oof:tt> was set.
		If a connection to the MDS is ever lost, the clients are themselves
		responsible for reestablishing and reissuing requests.
	</oof:p>

	<oof:header size='2'>Development tasks</oof:header>
	<oof:p>
		In either case, after any bmaps have been marked as
		<oof:tt>GARBAGE</oof:tt>, <ref sect='p'>upsch</ref> should schedule
		work to reclaim space for file holes created for any sliods that
		support operations such as fallocate(2).
		After notifications, the MDS can free any such bmaps.
	</oof:p>
</xdc>
