<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>SLASH2 update scheduler</title>

	<h1>Overview</h1>
	<p>
		The purpose of the update scheduler (upsch) is to provide reliable,
		user-controlled replication between two I/O systems.
	</p>
	<p>upsch has the following characteristics:</p>
	<ul>
		<li>
			Resume automatically after a reboot of the metadata server.
		</li>
		<li>
			Load balance between available source and destination resources.
		</li>
		<li>
			Be able to indicate/track completion status.
		</li>
		<li>
			Must handle unreliable source and destination nodes.
		</li>
		<li>
			Update metadata server's block replication tables as individual
			blocks are replicated.
		</li>
	</ul>

	<h1>Implementation</h1>
	<p>
		Replication requests will be issued serially by the client.
		This means that for the replication of an entire directory, the
		client will be responsible for iterating the directory's contents
		and issuing a request for each file.
	</p>
	<p>
		For each replication request received by the metadata server, a
		replication object will be created which is based on the FID of the
		file.
		While the FID is locked, its metadata structures will be iterated.
		For each structure or block map (bmap) a replication specific
		stucture will be created for that bmap.
		These bmap replication structures will be used to store the
		transactional replication state for that block.
	</p>
	<p>
		Replication objects should be created in a common area so that all
		pending/processing replications are grouped together.
		Combing through the entire namespace searching for replication
		objects will not work.
	</p>
	<p>
		After the replication object is created, a request queue consisting
		of the bmap replication structures is made.
		At this time the io server(s) at the destination are contacted by
		the mds with instruction to pull the replication requests from the
		queue.
		As the requests are pulled, the mds marks the transactional state in
		the appropriate bmap replication structure.
	</p>
	<p>
		When the io server reports completion the MDS does the following
		tasks:
	</p>
	<ul>
		<li>
			Marks completion in the bmap replication structure (for that
			bmap).
		</li>
		<li>
     Updates the file's metadata bmap replication table to signify that
		 the block has been replicated and is ready for service.
		</li>
	</ul>

	<h1>Development Tasks</h1>
	<ul>
		<li>
			Client Rpc Request (to MDS) for Replication
			<ul>
				<li>
					RPC which handles pathname
				<li>
			</ul>
		</li>
		<li>
			MDS backend RPC handler (performs what is outlined above)
		</li>
		<li>
			IOS backend RPC handler for pulling in replication requests
		</li>
		<li>
			MDS backend RPC handler for processing IOS replication activities.
		</li>
	</ul>

	<h1>Outstanding Issues</h1>
	<ul>
		<li>Source-side load balancing</li>
		<li>
			Replication object cleanup and monitoring
			<ul>
				<li>
					how do we ensure that requests complete
				</li>
				<li>
					what to do about stalls?
				</li>
			</ul>
		</li>
		<li>
			Consider the ramifications of ongoing file updates while its
			blocks are being replicated.
		</li>
	</ul>
</xdc>
