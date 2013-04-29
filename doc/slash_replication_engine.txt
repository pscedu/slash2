<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

Slash Replication Engine (SlRE)
-------------------------------

The purpose of the SlRE is to provide reliable, user-controlled replication
between two sets of Slash Object Storage resources.

SlRE must have the following characteristics:
     . Resume automatically after a reboot of the metadata server.
     . Load balance between available source and destination resources.
     . Be able to indicate / track completion status.
     . Must handle unreliable source and destination nodes.
     . Update metadata server's block replication tables as individual
     blocks are replicated.


Proposed Methodology
--------------------
Replication requests will be issued serially by the client.  This means that
for the replication of an entire directory, the client will be responsible
for iterating the directory's contents and issuing a request for each file.

For each replication request received by the metadata server, a replication
object will be created which is based on the FID of the file.  While the FID
is locked, its metadata structures will be iterated.  For each structure
or block map (bmap) a replication specific stucture will be created for that
bmap.  These bmap replication structures will be used to store the
transactional replication state for that block.

Replication objects should be created in a common area so that all pending /
processing replications are grouped together.  Combing through the entire
namespace searching for replication objects will not work.

After the replication object is created, a request queue consisting of the
bmap replication structures is made.  At this time the io server(s) at the
destination are contacted by the mds with instruction to pull the
replication requests from the queue.  As the requests are pulled, the mds
marks the transactional state in the appropriate bmap replication structure.
When the io server reports completion the mds:
     1) Marks completion in the bmap replication structure (for that bmap).
     2) Updates the file's metadata bmap replication table to signify that
     the block has been replicated and is ready for service.

Development Tasks
-----------------
. Client Rpc Request (to MDS) for Replication
  . RPC which handles pathname

. MDS backend Rpc handler (performs what is outlined above).

. IOS backend Rpc handler for pulling in replication requests.

. MDS backend Rpc handler for processing IOS replication activities.

Outstanding Issues
------------------
. Source-side load balancing.
. Replication object cleanup and monitoring.
  . how do we ensure that requests complete
  . what to do about stalls?
. Consider the ramifications of ongoing file updates while its blocks are
  being replicated.

