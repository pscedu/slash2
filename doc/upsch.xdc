<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Update scheduler</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		The purpose of the update scheduler (upsch) is to provide reliable,
		user-controlled replication between two I/O systems.
	</oof:p>
	<oof:p>upsch has the following characteristics:</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Resume automatically after a reboot of the metadata server.
		</oof:list-item>
		<oof:list-item>
			Load balance between available source and destination resources.
		</oof:list-item>
		<oof:list-item>
			Be able to indicate/track completion status.
		</oof:list-item>
		<oof:list-item>
			Must handle unreliable source and destination nodes.
		</oof:list-item>
		<oof:list-item>
			Update metadata server's block replication tables as individual
			blocks are replicated.
		</oof:list-item>
	</oof:list>

	<oof:header size="1">Implementation</oof:header>
	<oof:p>
		Replication requests will be issued serially by the client.
		This means that for the replication of an entire directory, the
		client will be responsible for iterating the directory's contents
		and issuing a request for each file.
	</oof:p>
	<oof:p>
		For each replication request received by the metadata server, a
		replication object will be created which is based on the FID of the
		file.
		While the FID is locked, its metadata structures will be iterated.
		For each structure or block map (bmap) a replication specific
		stucture will be created for that bmap.
		These bmap replication structures will be used to store the
		transactional replication state for that block.
	</oof:p>
	<oof:p>
		Replication objects should be created in a common area so that all
		pending/processing replications are grouped together.
		Combing through the entire namespace searching for replication
		objects will not work.
	</oof:p>
	<oof:p>
		After the replication object is created, a request queue consisting
		of the bmap replication structures is made.
		At this time the io server(s) at the destination are contacted by
		the mds with instruction to pull the replication requests from the
		queue.
		As the requests are pulled, the mds marks the transactional state in
		the appropriate bmap replication structure.
	</oof:p>
	<oof:p>
		When the io server reports completion the MDS does the following
		tasks:
	</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Marks completion in the bmap replication structure (for that
			bmap).
		</oof:list-item>
		<oof:list-item>
     Updates the file's metadata bmap replication table to signify that
		 the block has been replicated and is ready for service.
		</oof:list-item>
	</oof:list>

	<oof:header size="1">Development Tasks</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>
			Client Rpc Request (to MDS) for Replication
			<oof:list type="LIST_UN">
				<oof:list-item>
					RPC which handles pathname
				</oof:list-item>
			</oof:list>
		</oof:list-item>
		<oof:list-item>
			MDS backend RPC handler (performs what is outlined above)
		</oof:list-item>
		<oof:list-item>
			IOS backend RPC handler for pulling in replication requests
		</oof:list-item>
		<oof:list-item>
			MDS backend RPC handler for processing IOS replication activities.
		</oof:list-item>
	</oof:list>

	<oof:header size="1">Outstanding Issues</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>Source-side load balancing</oof:list-item>
		<oof:list-item>
			Replication object cleanup and monitoring
			<oof:list type="LIST_UN">
				<oof:list-item>
					how do we ensure that requests complete
				</oof:list-item>
				<oof:list-item>
					what to do about stalls?
				</oof:list-item>
			</oof:list>
		</oof:list-item>
		<oof:list-item>
			Consider the ramifications of ongoing file updates while its
			blocks are being replicated.
		</oof:list-item>
	</oof:list>
</xdc>
