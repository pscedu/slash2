<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

	<h1>Overview</h1>
	<p>
		This document describes the implementation of truncate operates in
		the SLASH2 system.
	</p>
	<p>
		There are two kinds of truncates that are handled differently:
	</p>
	<ul>
    <li>
			full truncate, i.e. truncate to file offset position 0
    </li>
    <li>
			partial truncate, i.e. truncate to non-zero file offset position
		</li>
	</ul>

	<p>
		Full truncates cause the file generation number to be bumped and
		record the old file ID/generation# into a logfile which is shipped
		to all sliods that were registered in the file replica table.
		Once these logfiles are successfully received by all sliods they
		pertain to and the sliods reply, signifying the actions have been
		applied, the logfiles on the MDS are deleted.
	</p>
	<p>
		Other actions that evoke this same garbage collection mechanism are:
	</p>

	<ul>
		<li>unlink(2)</li>
		<li>a clobbering rename(2)</li>
	</ul>

	<p>
		Partial truncates are performed solely by the truncate(2) system
		call and only affects the current metadata in the MDS of the
		affected file.
	</p>
	<p>
		First, if the file is marked IN_PTRUNC, signifying that it is
		already handling partial truncation resolution (i.e. the steps
		outlined here), failure is returned immediately to the client
		issuing the SETATTR.
	</p>
	<p>
		The file is marked (in memory only, at this point) IN_PTRUNC and the
		client is notified that this behavior is taking place.
		At this point, it is the client's responsibility to reissue the
		SETATTR on failure as the MDS provides no guarentees yet that the
		operation will be recorded by a journal or other persistent behavior
		tracker.
	</p>
	<p>
		Next, any leases currently granted to clients for bmaps including or
		falling after the partial truncate file offset position (hereby
		referred to as the 'ptrunc position') are asked to be released by
		the MDS.
	</p>
	<p>
		The MDS then waits for clients to relinquish all said leases,
		waiting a maximum of the bmap timeout time in the case of
		unresponsive clients.
	</p>
	<p>
		The next action is determined depending on the value of the ptrunc
		position:
	</p>

	<ul>
		<li>
			if the ptrunc position falls cleanly between two bmaps, the
			following actions are taken:

			<ul>
				<li>
					bmaps after the ptrunc position are marked VALID &rarr;
					GARBAGE and written to ZFS,
				</li>
				<li>
					the new file size is saved in sst_size and written to the ZFS
					metadata backend file system without journaling, and
				</li>
				<li>
					the IN_PTRUNC flag is cleared from the file, and
				</li>
			</ul>
		</li>
		<li>
			if the ptrunc position falls within a bmap, USSR work is queued to
			resolve the CRC recalculation that must occur before processing
			can return to normal.
		</li>
	</ul>

	<p>
		All full bmaps past the ptrunc position are marked VALID &rarr;
		GARBAGE and the bmap where the ptrunc position lies is marked VALID
		&rarr; TRUNCPNDG.
	</p>

	<p>
		At earliest convenience, a randomly selected TRUNCPNDG sliod is
		asked to perform the CRC recalculation.
		When one finally does, the bmap is marked TRUNCPNDG->VALID and other
		replicas are marked TRUNCPNDG->GARBAGE.
	</p>

	<p>
		At this point, the MDS issues BMAP_WAKE notifications to the
		original client as well as to any new clients that attempted SETATTR
		or BMAP_LEASE requests since IN_PTRUNC was set.
		If a connection to the MDS is ever lost, the clients are responsible
		for reestablishing and reissuing requests to become "registered" for
		BMAP_WAKE callback when the resolution finishes.
	</p>

	<p>
		In either case, after any bmaps have been marked as GARBAGE, USSR
		work is scheduled for garbage reclamation for those bmaps at
		earliest convenience.
		Once all sliods have notified the MDS that the garbage has been
		reclaimed, the MDS truncates any entirely freed bmaps from the
		backend metadata file.
	</p>
</xdc>
