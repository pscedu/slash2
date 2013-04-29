<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

	<h1>MDS configuration considerations</h1>
	<h2>Selecting storage for SLASH2 metadata</h2>
	<p>
		A metadata server should employ at least one dedicated disk (or
		partition) for a ZFS pool and a dedicated disk for journal.
		It is important for the journal be on its own spindle if one expects
		decent performance.
		If fault tolerance is required then at least 2 devices will be
		required for the ZFS pool.
	</p>
	<p>
		Solid state devices (SSD) are suitable and recommended for SLASH2 metadata storage.
	</p>
	<p>
		Here is an example zpool configuration taken from the PSC Data
		Supercell.
		Here the SLASH2 MDS sits atop 2 vdevs, each of which is
		triplicated:
	</p>
	<pre>
# zpool status
  pool: arc_s2mds
 state: ONLINE
 scrub: none requested
config:

	NAME                                   STATE     READ WRITE CKSUM
	arc_s2mds                              ONLINE       0     0     0
	  mirror-0                             ONLINE       0     0     0
	    disk/by-id/scsi-35000c50033f6624b  ONLINE       0     0     0
	    disk/by-id/scsi-35000c50033f64b0f  ONLINE       0     0     0
	    disk/by-id/scsi-35000c50033f6439f  ONLINE       0     0     0
	  mirror-1                             ONLINE       0     0     0
	    disk/by-id/scsi-35000c500044205df  ONLINE       0     0     0
	    disk/by-id/scsi-35000c50033ea560b  ONLINE       0     0     0
	    disk/by-id/scsi-35000c50033f63417  ONLINE       0     0     0

errors: No known data errors

# zpool iostat
	       capacity     operations    bandwidth
pool        alloc   free   read  write   read  write
----------  -----  -----  -----  -----  -----  -----
arc_s2mds    379G  3.26T    493     70  2.52M   351K
</pre>

	<h2>How much storage is required for metadata?</h2>
	<p>
			PSC's Data Supercell has on the order of 10^8 files and
			directories.
			The ZFS storage required to store these metadata items is about
			300GB (uncompressed).
			Using the default ZFS compression we generally see about 3.5 : 1
			compression of SLASH2 metadata.
			Without compression one should expect about 150k files
			to consume about 1GB of ZFS metadata storage.
			With compression, that ratio would improve to about 500k files per
			1GB of ZFS metadata storage.
	</p>

	<h2>How much storage is required for the metadata journal?</h2>
	<p>256MB - 2GB</p>

	<h2>Should the journal device be mirrored?</h2>
	<p>
		Losing a journal will not result in a loss of the filesystem, only
		the uncommitted changes would be lost.
		If mirroring is desired, the linux MD mirroring is suitable though
		we recommend that the devices are partitioned so that rebuilds of
		the mirror do not require a rebuild of the entire device.
	</p>

	<h2>Creating an MDS ZFS pool and journal</h2>
	<p>
		The following steps create and tailor a ZFS pool for use as SLASH2
		metadata storage.
		These steps are also documented in <ref sect="7">sladm</ref>.
		<ref sect="8">slmkfs</ref> will output a UUID which will become the
		identifier for the filesystem.
		This hex value is needed for the SLASH2 configuration file and for
		I/O server setup.
	</p>
	<p>
		Warning!
		Use devices which pertain to your system and are not currently in
		use!
		It is recommended to use the global device identifiers listed in
		/dev/disk/by-id, especially for the journal.
		This may prevent you accidentally trashing another mounted
		filesystem.
	</p>
	<pre>
$ zfs-fuse &amp;&amp; sleep 3
$ zpool create -f s2mds_pool mirror /dev/sdX1 /dev/sdX2
$ zfs set compression=on s2mds_pool
$ slmkfs /s2mds_pool
  The UUID of the pool is 0x2a8ae931a776366e
$ pkill zfs-fuse

# Create the journal on a separate device with the UUID output by
#   slmkfs.  The journal created by this command will be 512MiB.

$ slmkjrnl -f -b /dev/sdJ1 -n 1048576 -u 0x2a8ae931a776366e
</pre>

	<h1>Network configuration</h1>
	<p>
		SLASH2 uses the Lustre networking stack (aka LNet) so configuration
		will be somewhat familiar to those who use Lustre.
		At time time SLASH2 supports TCP and Infiniband sockets direct
		(SDP).
		It does not currently support LNet routing in a stable manner (this
		could be fixed if someone needs it) however, mixed topologies are
		supported to some degree.
		For instance, if clients and I/O servers have infiniband and
		ethernet they may use IB for communication even if the MDS has only
		an ethernet link.
	</p>

	<h1>SLASH2 Configuration</h1>
	<p>
		Example configurations are provided in projects/slash_nara/config.
		The slcfg(5) man page also contains more information on this topic.
	</p>

	<h1>Setting zpool, fsuuid and network globals</h1>
	<p>
		The previous 2 steps will provide the parameters for the 'fsuuid',
		'port', and 'nets' attributes.
		As configured here, the port used by the filesystem's tcp
		connections will be 989 (non-priviledged ports are allowed).
		The LNet network identifier for the tcp network is 'tcp0'.
		Any clients or servers with interfaces on the 192.168 network will
		match the rule '192.168.*.*' and be configured on the tcp0 network.
		Hosts with infininband interfaces on the 10.0.0.* network will be
		configured on the sdp0 network.
	</p>
	<p>
		For now leave 'pref_mds' and 'pref_ios' blank.
	</p>
	<pre>
set zpool_name="s2mds_pool";
set fsuuid="2a8ae931a776366e";
set port=989;
set nets="tcp0 192.168.*.*; sdp0 10.0.0.*";
#set pref_mds="";
#set pref_ios="";
</pre>

	<h2>Configuring a SLASH2 site</h2>
	<p>
		A site is considered to be a management domain in the cloud or
		wide-area.
		slcfg(5) details the resource types and the example configurations
		in the distribution may be used as guides.
		Here we'll do a walk through of a simple site configuration.
	<p>
	<pre>
set zpool_name="s2mds_pool";
set fsuuid="2a8ae931a776366e";
set port=989;
set nets="tcp0 192.168.*.*; sdp0 10.0.0.*";
set pref_mds="mds@MYSITE";
set pref_ios="ion1@MYSITE";

site @MYSITE {
     site_desc = "test SLASH2 site configuration";
     site_id   = 1;

     # MDS resource
     resource mds {
	      desc = "my metadata server";
	      type = mds;
	      id   = 0;
	      # 'nids' should be the ip or hostname of your mds node.  It
	      # should be on the network specified in the global 'nets'
	      # variable above.
	      nids = 192.168.0.100;
	      # 'jrnldev' matches the device we formatted above.
	      jrnldev = /dev/sdJ1;
     }

     resource ion1 {
	      desc = "I/O server 1";
	      type = standalone_fs;
	      id   = 1;
	      nids = 192.168.0.101, 10.0.0.1;
	      # 'fsroot' points to the storage mounted on the I/O server
	      #  which is to be used by SLASH2
	      fsroot = /disk;
     }

     resource ion2 {
	      desc = "I/O server 2";
	      type = standalone_fs;
	      id   = 2;
	      nids = 192.168.0.102, 10.0.0.2;
	      fsroot = /disk;
     }
}
</pre>

	<p>
		Note that the <tt>pref_mds</tt> and <tt>pref_ios</tt> global values
		have been filled in based on the names specified in the site.
		The <tt>pref_ios</tt> is important because it is by clients as the
		default target when writing new files.
	</p>

	<h1>I/O Server Setup</h1>
	<p>
		The configuration above lists 2 SLASH2 I/O servers, ion1 and ion2.
		Both of which list their <tt>fsroot</tt> as <tt>/disk</tt>.
		The SLASH2 I/O server is a stateless process which exports locally
		mounted storage into the respective SLASH2 file system.
		In this case, we assume that <tt>/disk</tt> is a mounted filesystem
		with some available storage behind it.
	</p>
	<p>
		In order to use the storage mounted at <tt>/disk</tt>, a specific
		directory structure must be created with the slmkfs command prior to
		starting
		the I/O server.
		The UUID of the filesystem must be supplied as a parameter.
		Per this example, the following command is run on both ion1 and
		ion2:
	</p>
	<pre>
# slmkfs -i -u 0x2a8ae931a776366e /disk
$ ls -l /disk/.slmd/
total 4
drwx------ 3 root root 4096 Jun 18 15:10 2a8ae931a776366e
</pre>

	<p>
		Once completed, the directory <tt>/disk/.slmd</tt> should appear
		which contains a subdirectory named for theUUID.
		Under <tt>/disk/.slmd</tt> is a hierarchy of 16^4 directories used
		for storing SLASH2 file objects.
	</p>
	<p>
		An explanation of more sophisticated I/O system types is given here:
		SLASH2IOServices
		RunningSLASH2Services
	</p>
</xdc>
