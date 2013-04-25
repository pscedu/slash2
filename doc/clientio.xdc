<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Binding Bmaps to I/O Nodes</title>

IOS Load Balancing: How to choose an import for a given read/write operation
Two conflicting phenomena must be managed:
Exploiting server-side caches
Not flooding a single server with requests
	<oof:p>
		To complicate matters, we don't have a method for performance
		sharing between clients, so some other means may be necessary such
		as storing real-time performance state.
	</oof:p>
	<oof:p>
		Also, a method may be needed to make load-balancing / throughput
		decisions which take into account several I/O Systems (applies only
		to read). One example would be "3/4 servers are down on the
		preferred IOS.. should we start to forward requests to secondary or
		tertiary IOS's?" Another could be "fileA is stored on a archiver at
		local-site and parallel-fs on remote-site - which site is chosen?
	</oof:p>
	<oof:p>
		Perhaps the IOS's themselves could maintain statistics (long term or
		just run-time) which summarize their ability to process different
		types and sizes of IO requests?
	</oof:p>

	Size range (very small, small, medium, large, very large)
	<oof:p>
		Summaries would exist on a per-site (or even per-client) basis and
		the statistics should separate disk I/O from network latency (so
		that poorly performing or remote clients do not interfere with disk
		I/O readings).
	</oof:p>
	<oof:p>
		On the client the summaries would be attached to their respective
		IOS structures and consulted when a read request is being issued.
		Note that writes are different because the client is required to
		bind a write operations (on a per-bmap basis) with a single IOS.
	</oof:p>





Read-before-write: Client-side buffering mechanism
	<oof:p>
		Revisions 3572 and 3505 of mount_slash_int.c attempt to implement a
		caching mechanism where the first and last pages can be cached
		independently of the rest of offtree_memb iov. This is for dealing
		with writes which start (or end) within a SLASH2 slab page. The code
		was very ugly because there is no storage for tracking inflight
		state of a slab block - only an iov. Worse yet it was determined
		that for writes which start in the middle of a large iov (lots of
		blocks) no method exists for informing other cache threads that the
		ios is only partially valid.
	</oof:p>
	<oof:p>
		With this in mind, I've decided to prefetch the entire iov -
		regardless of its size - when handling read-before-write. This
		should serve fine for a prototype. Since the client cache is
		complicated, the logic for this be handled in the initial offtree
		allocation. For instance, unaligned overwrites may be detected in
		the first call into offtree which makes possible the ability to
		allocate iov's accordingly.
	</oof:p>
	<oof:p>
		For example, if an overwrite requires RBW at the first and last
		pages then the offtree can allocate single block iov's at the
		beginning and end allowing the rest of the cache subsystem to
		function in a straight-forward manner. For large iov's which have
		already been allocated the situation is more grim, though if we
		wanted, the large iov's could be forcibly split across children.
	</oof:p>




Dealing with parallel I/O servers: What are the issues involved?
	<oof:p>
		Parallel IOServers describes a system which has a set of symmetric
		clients where the clients have a consistent or coherent view of the
		filesystem.
	</oof:p>
For example:
   resource bessemer {
      desc = "DDN9550 Lustre Parallel Fs";
      type = parallel_fs;
      id   = 0;
      ifs  =  128.182.112.110,
	 128.182.112.111,
	 128.182.112.112,
	 128.182.112.113;
      peers = bigben@PSC, golem@PSC;
   }

	<oof:p>
		An example case would be where a client issues a write to IOS_a0 and
		then to IOS_a1 for regions which fall into the same bmap. Upon the
		first write into the bmap, the IOS notifies the mds that the
		generation number must be bumped, denoting that the bmap chunk has
		been modified (and therefore outdating the other replicas). The
		client, being fickle, issues a subsequent write to a peer IOS
		(IOS_a1) which falls into the same bmap. Now since the backing
		filesystem is coherent, there is no need to bump the generation
		number again. Since the IOS's do not explicitly communicate, the
		client must inform IOS_a1 that the bmap he's accessing has already
		been bumped and therefore the IOS should process the write without
		communication to the mds (for the purpose of bump gen - crc related
		communications will still persist).
	</oof:p>
	<oof:p>
		Therefore, after first write, the client must present a token
		denoting that first access ops have already been handled. Of course
		this only applies when peer systems are connected via a coherent
		backing fs. In addition, IOS's in coherent environments must always
		flush their slash buffers to the fs.
	</oof:p>
	<oof:p>
		In the case where we have a set of stand-alone nodes, such as the
		lcn cluster, these must be treated differently since they are not
		backed by a shared fs. The result being that clients cannot update
		bmaps on different IOS's without bumping the generation number and,
		in essence, canceling out writes to peer IOS's.
	</oof:p>
Design Fallouts:
	<oof:p>
		Synchronous writes - before returning success on write to the
		client, the IOS must ensure its buffers are written.
	</oof:p>
	<oof:p>
		Bmap first write token - this token should originate at the mds (via
		the shared-secret mechanism) and be verifiable by the IOS. This way
		the IOS does not trust the client.
	</oof:p>
	<oof:p>
		After thinking about CrcManagement a bit more, I've concluded that
		parallel IOS will do more harm than good. This is because there is
		no way to serialize / synchronize the CRC state on disk with that on
		the MDs. Therefore the approach described in CrcManagement will be
		taken. The basic idea is that the MDS binds a bmap to an IOS and
		redirects all other nodes to that IOS. Hence, that IOS is the only
		one who make update the CRC tables for the given bmap.
	</oof:p>
	<oof:p>
		The design fallouts described above still apply, the difference
		being that the write token is no longer shareable.
	</oof:p>
	<oof:p>
		Note that this does not apply to read().
	</oof:p>
</xdc>
