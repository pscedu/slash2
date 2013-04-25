<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>I/O server write request processing</title>
	<oof:p>
		For both read and write requests the CRC is the dominant activity
		around which decisions regarding buffering and disk I/O must be
		made.
		Regardless of the size of the write, which must be less than
		LNET_MTU, the IOD must read an entire 'sliver' of data so that a CRC
		may be taken.
		A sliver is a sub-unit of the bmap.
		Presently we assume that all slivers are aligned, 1MB chunks.
		The process should be the same for DIO and non-DIO bmaps.
	</oof:p>
	<oof:p>
		Upon receiving a bmap write request the IOD first does a lookup on
		the sliver or slivers which are affected by the incoming write.
		If they are not present then they are created and tagged with an
		initialization bit.
		While the init bit is set, only 'this' thread may work on the new
		sliver reference.
		During the initialization process, newly created slivers are backed
		by slab buffers which are allocated from the slab cache.
		Once the slab has been allocated I/O into the region may begin.
		The filewise offset can be calculated by:
	</oof:p>
	<oof:pre>((bmapno * bmapsz) + (slvrno * slvrsz))</oof:pre>
	<oof:p>
		Writes targeted for an uncached sliver invoke a read of the entire
		sliver (whether we CRC this sliver prior to modification should be
		discussed - for now we assume no).
		As the sliver region is modified by the write the sliver is
		scheduled to be CRC'd, the backing slab buffer may not freed or
		reallocated until the CRC has been taken.
		sliod tries to delay CRC'ing of slivers to avoid taking multiple
		CRCs of the same sliver in a relatively short time period.
		Small writes require that only the dirtied slab section is written
		to disk (though the entire must be CRC'd).
		The slb_inuse bitmap is used to track the dirty sections of the
		slab.
		Remember that sliver writes are 'write-through'.
	<oof:p>
	</oof:p>
		On read, once a sliver has been CRC'd any sub-unit of that sliver
		may be used for reading without requiring any further CRCs.
	</oof:p>
</xdc>
