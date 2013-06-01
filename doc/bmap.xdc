<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Bmap metadata and user data CRCs</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		A bmap has its generation number bumped once it receives a CRC
		update from a ION, which the MDS has designated as the 'allowable
		writer', and the bmap has been replicated to other.
		bumpgen() serves the purpose of nullifying the bmap replicas which
		have no been made obsolete by the write.
	</oof:p>
	<oof:p>
		It should be noted that only one ION may perform bumpgen() on a
		bmap.
		This ION is serially chosen by the MDS, so long as this MDS -> ION
		association is in place, no other IONs may issue CRC updates to
		the MDS.
	</oof:p>
	<oof:p>
		Some issues relating to data structures hang in the balance.
		At this time I feel that decisions pertaining to bandwidth
		(disk/network) versus metadata capacity are the most critical.
	</oof:p>

	<oof:header size="1">MDS Bmap Management</oof:header>
	<oof:p>
		When a client issues a read or write command, an rpc to the MDS is
		made requesting the BMAP for the associated file region.
		This document describes the metadata processes involved when
		handling a bmap request from the client.
	</oof:p>

	<oof:header size="1">Bmap Cache Lookup</oof:header>
	<oof:p>
		Upon receipt of the BMAP request, the MDS verifies that the issuing
		client actually has an open fd for the given FID.
		This is checked in two places:
	</oof:p>
	<oof:p>
		The fidc_open_object (attached to the fcmh) must be allocated.
		This denotes that the fid is open.
	</oof:p>
	<oof:p>
		Attached to the CFD must be a struct mexpfcm (this is the per client
		data structure which tracks which fid's a client is using).
		mexpfcm contains a tree of mexpbcm, which track which bmaps (on this
		fid) are in use by the client.
		The client should not request a bmap for which an mexpbcm already
		exists unless the mode is changing from read to write or vise versa.
	</oof:p>
	<oof:p>
		Note: Only on read or write should the bmap tracking incur a log
		operation.
		Otherwise, things like 'touch' will cause an unnecessary log
		operation.
	</oof:p>
	<oof:p>
		Once the fidc_membh and cfd are determined to be correct, the
		fidc_membh's open object (fcmh_fcoo) is accessed.
		The fcmh_fcoo contains a splay tree of cached bmaps (struct
		bmap_cache fcoo_bmapc).
		The bmap cache lookup consists of searching the tree
		for the bmap number which corresponds to the request.
		If the bmap is located this means that another client has or is
		accessing this bmap.
		Hence, a mexpbcm is allocated and stored in the bmap's struct
		bmap_mds_info which also has a tree for indexing the exports which
		access this bmap.
		Note that upon adding the mexpbcm, an ondisk reference to the bmap
		lease must be recorded.
		The purpose of this record serves to rebuild the server lease state
		upon reboot or failover.
	</oof:p>
	<oof:p>
		Note: the cfd number assigned to the client has to be preserved in
		the log.
		Additionally, the restored cfd number must be taken into account for
		when allocating new cfd's to that export.
	</oof:p>
	<oof:p>
		If the bmap does not exist in the cache then it must be retrieved
		from disk but first a placeholder bmap is inserted into the cache to
		prevent multiple threads from performing the same I/O.
		Other threads waiting for the bmap block on the placeholder bmap's
		bcm_waitq.
	</oof:p>
	<oof:p>
		bmaps are fixed size structures.
		To read a specific bmap from an inode's metafile requires the bmap
		index.
		The bmap number multiplied by the bmap ondisk size gives the offset
		into the metafile.
		At this time it should be noted that the metafile file size does not
		accurately reflect the number of bmaps stored in the metafile.
		The file size is used to record the slash2 file's actual size (this
		is done for performance reasons).
		With that in mind, it is possible and likely that bmaps will be read
		from regions of the metafile which have not yet been written.
		In fact, this will happen each time a file is written to for the
		first time.
		So there must be a way to detect when a read bmap is just an array
		of nulls (i.e. bytes read from a non-existent part of the file).
		The null ondisk bmap is not initialized until it must store some
		real data.
		At the time when an ION processes a write for this bmap and sends
		the MDS the bmap's crcs, the MDS is then required to store an
		initialized bmap at the respective index.
	</oof:p>

	<oof:header size="1">Bmap Logging</oof:header>
	<oof:p>
		At present, the model for logging all bmap leases will rely on a
		per-fid collapsible journal.
		For each fid which has exports accessing it, and its bmaps, the mds
		must log the bmap lessees for recovery purposes.
		On recovery, what essentially happens is that these logs are
		replayed to recreate the MDS's export cache.
	</oof:p>

	<oof:header size="1">Change Logging</oof:header>
	<oof:p>
		XXX this belongs in a different file.
		All modifications to directory inodes, file inodes, or bmaps, must
		be recorded in a transaction log for mds replication purposes.
		This should be done a using sljournal code.
	</oof:p>
</xdc>
