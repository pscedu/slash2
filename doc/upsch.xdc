<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Update scheduler</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		The update scheduler (upsch) is a scheduling engine that coordinates
		the transmission of information and instructions to peers on a
		SLASH2 network.
		Such activity includes garbage reclamation and replication
		arrangements.
	</oof:p>
	<oof:p>upsch has the following features:</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Resume automatically after a reboot of the metadata server.
		</oof:list-item>
		<oof:list-item>
			For replication, load balance between available source and
			destination resources.
		</oof:list-item>
		<oof:list-item>
			Be able to indicate/track live replication status.
		</oof:list-item>
		<oof:list-item>
			Must handle unreliable source and destination nodes during
			communication.
		</oof:list-item>
		<oof:list-item>
			Update metadata as notifications are received from peers.
		</oof:list-item>
	</oof:list>

	<oof:header size="1">Implementation</oof:header>
	<oof:p>
		Replication requests are issued by the client on a per-bmap,
		per-destination basis.
		For example, if a user wishes to replicate the contents of all data
		in all files hierarchically below his directory to another IOS,
		msctl will recursively examine each file and serially issues
		requests for each bmap.
		Short hand notation of bmapno equal to -1 means that the replication
		operation should apply to all bmaps belonging to the specified file.
	</oof:p>
	<oof:p>
		Replication requests received by the MDS are treated like any other
		update that the engine must process:
		<oof:list>
			<oof:list-item>
				register a new replica in the file's replica table if the
				destination IOS is not already listed in this table
			</oof:list-item>
			<oof:list-item>
				load the bmap metadata, sanity check the pending operation, and
				update the bmap replica table to the QUEUED state for this IOS
			</oof:list-item>
			<oof:list-item>
				update the upsch database by adding an entry for this piece of
				work
			</oof:list-item>
		</oof:list>
	</oof:p>
	<oof:p>
		Updates (work items) that the engine will process are specifically
		not all held in memory simultaneously by design.
		Instead, work is divided into two priority groups:
	</oof:p>
		<oof:list>
			<oof:list-item>
				critical/operational work, which do all stay in memory
			</oof:list-item>
			<oof:list-item>
				regular work, which stays in the database and is periodically
				paged in
			</oof:list-item>
		</oof:list>
	<oof:p>
		The upsch database is rebuilt upon startup or when corruption is
		detected.
	</oof:p>
	<oof:p>
		Critical work items are processed immediately by the engine.
		These types of work include cleaning up in-memory structures when an
		IOS connection goes down.
	</oof:p>
	<oof:p>
		At startup, or when a piece of work comes in (if the system is
		idle), or when bandwidth becomes available from the reservation
		system, a request to page-in actual work is triggered by the engine.
		The page-in operation consults the database looking for any
		applicable work to perform.
		For each IOS destined for updates, a maximum number of regular work
		items may be paged in.
	</oof:p>
	<oof:p>
		Regular work is divided into classes to prioritize scheduling.
		The classes include administrator priority levels and user priority
		levels.
		Work requests in the same class have the same likelihood of being
		selected.
		The class levels are so designed to give administrator priority,
		which takes first precedence, as well as user priority for enqueued
		activity.
	</oof:p>
	<oof:p>
		When a piece of work is processed, the IOS is contacted and
		instructed by the given update.
		In the case of replication arrangement, he is told where to pull
		from, as the arrangements provided by the MDS are respectful of any
		bandwidth reservation policies.
		Metadata in the MDS is updated so any such operations do not reoccur
		and remembered in case communication is severed and need to be
		reissued once the link becomes available once again.
	</oof:p>
	<oof:p>
		When an IOS reports completion the MDS updates any metadata; e.g. a
		completed replication marks the bmap metadata for this IOS in its
		replica table from QUEUED to VALID (if the IOS reported success)
	</oof:p>

	<oof:header size="1">Development tasks</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Update work request administrator priority.
			Not implemented as described above; only user priority is
			currently implemented.
		</oof:list-item>
		<oof:list-item>
			Dynamic bandwidth reservation policy modification.
			Must have the ability to change the link bandwidth reservations on
			the fly.
		</oof:list-item>
		<oof:list-item>
			Source-side load balancing.
			Currently, source IOS's are selected at random, respectful of any
			bandwidth reservation policies.
		</oof:list-item>
		<oof:list-item>
			Replication object cleanup.
			Use of fallocate(2) needs to be leveraged to reclaim space.
		</oof:list-item>
	</oof:list>
</xdc>
