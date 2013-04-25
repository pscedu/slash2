<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>User data CRC management</title>

CRC Management: Calculation, communication, storage, retrieval
The dominant issue with CRC's revolves around the data size encompassed by a single 8byte CRC. This has direct ramifications in the amount of buffering required and the mds capacity needed to store CRCs. Also since the MDS stores the CRCs, the system ingest bandwidth is essentially limited to the number of CRCs the MDS can process. Issues regarding the synchronous storing of MDS-side CRCs need to be explored. For our purposes we will assume that the MDS has safely stored the CRCs before acknowledging back to the IOS.
MDS CRC Storage
The MDS has a fixed size array for CRC storage, the array size if the product of the CRC granularity and the bmap size. For now we assume that the bmap size is 128mb and the granularity is 1MB, so the array size 1k per bmap. Here we can see that 8bytes per 1MB provides a reasonable growth path for CRC storage.
 (1024^2/(1024^2))*8 // 8bytes per MB
 8
 (1024^3/(1024^2))*8 // 1GB requires 8KB of CRCs
 8192
 (1024^4/(1024^2))*8
 8388608
 (1024^5/(1024^2))*8 // 1PB requires 8GB of CRCs
 8589934592
 (1024^6/(1024^2))*8 // 1EB requires 8TB of CRCs
 8796093022208
IOS CRC
As writes are processed by the IOS we must ensure that the CRCs are accurate and take into account any cache coherency issues that may arise. One problem we face with Parallel IOS and CRCs is that we have no way to guarantee which IOS wrote last and therefore which CRCs accurately reflect the state of the file. Therefore, revising the parallel IOS write protocol, the MDS will determine the IOS -> bmap association and provide the same IOS to all clients for a given bmap write session. This will ensure that only one source is valid for issuing CRC updates into a bmap region, and this source is verifiable by the MDS. There will surely be failure ramifications here... For instance:
Should the write occur but return with a failure the IOS must have a way of notifying the MDS that the CRC state on disk is unknown.
The IOS performs a write and then fails before sending the CRC update. The CRC should be calculated and stored before writing or sending to the MDS.
Synchronously delivered update RPC's will surely slow down the write process. Perhaps we should be able to batch an entire bmap's worth of updates. Also we should have a journal of client-side CRCs where possible to deal with failures, so that for any unsent CRCs (post-failure) may be verified the buffer-side crcs against the on-disk state and then update the MDS.
Need to consider what happens when an IOS fails from the perspective of the client and the MDS. The MDS may have to log/record bmap - IOS associations to protect against updates from a previous IOS ownership.
Design Fallouts:
MDS chooses IOS for a given bmap, CRC updates only come from that IOS.
This means that we can bulk crc updates up to the size of the bmap (big performance win).
Journal buffer-side CRCs (pre-write) to guard against IOS failure. (perhaps not..)
MDS RPC to IOS for calculating an entire bmap's worth of CRCs - this would be issued when an MDS detects the failure of an IOS and needs to reassign.
When an mds chooses an ION for write, he should notify other read clients of this.
</xdc>
