<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Configuring a SLASH2 deployment</title>

	<oof:header size="1">Configuring a SLASH2 deployment</oof:header>

	<oof:header size="2">MDS server</oof:header>

	<oof:header size="3">Selecting storage for SLASH2 metadata</oof:header>
	<oof:p>
		A metadata server should employ at least one dedicated disk (or
		partition) for a ZFS pool and a dedicated disk for journal.
		It is important for the journal to be on its own spindle if one
		expects decent performance.
		If fault tolerance is required then at least two devices will be
		required for the ZFS pool.
	</oof:p>
	<oof:p>
		Solid state devices (SSD) are suitable and recommended for SLASH2
		metadata storage.
	</oof:p>
	<oof:p>
		Here is an example zpool configuration taken from the PSC Data
		Supercell.
		Here the SLASH2 MDS sits atop two vdevs, each of which is
		triplicated:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zpool status
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

<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zpool iostat
	       capacity     operations    bandwidth
pool        alloc   free   read  write   read  write
----------  -----  -----  -----  -----  -----  -----
arc_s2mds    379G  3.26T    493     70  2.52M   351K
</oof:pre>

	<oof:header size="3">How much storage is required for metadata?</oof:header>
	<oof:p>
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
	</oof:p>

	<oof:header size="3">How much storage is required for the metadata journal?</oof:header>
	<oof:p>256MB - 2GB</oof:p>

	<oof:header size="3">Should the journal device be mirrored?</oof:header>
	<oof:p>
		Losing a journal will not result in a loss of the file system; only
		the uncommitted changes would be lost.
		If mirroring is desired, the Linux MD mirroring is suitable though
		we recommend that the devices are partitioned so that rebuilds of
		the mirror do not require a rebuild of the entire device.
	</oof:p>

	<oof:header size="3">Creating an MDS ZFS pool and journal</oof:header>
	<oof:p>
		The following steps create and tailor a ZFS pool for use as SLASH2
		metadata storage.
		These steps are also documented in <ref sect="7">sladm</ref>.
		<ref sect="8">slmkfs</ref> will output a UUID which will become the
		identifier for the file system.
		This hex value is needed for the SLASH2 configuration file and for
		I/O server setup.
	</oof:p>
	<oof:p>
		Warning!
		Use devices which pertain to your system and are not currently in
		use!
		It is recommended to use the global device identifiers listed in
		<oof:tt>/dev/disk/by-id</oof:tt>, especially for the journal.
		This may prevent you from accidentally trashing another mounted
		file system.
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zfs-fuse &amp;&amp; sleep 3
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zpool create -f s2mds_pool mirror /dev/sdX1 /dev/sdX2
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zfs set compression=on s2mds_pool
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> slmkfs -I $site_id /s2mds_pool
The UUID of the pool is 0x2a8ae931a776366e
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> pkill zfs-fuse

<oof:span class='syn_comment'># Create the journal on a separate device with the UUID output by</oof:span><oof:br />
<oof:span class='syn_comment'># slmkfs.  The journal created by this command will be 512MiB.</oof:span><oof:br /><oof:br />

<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> slmkjrnl -f -b /dev/sdJ1 -n 1048576 -u 0x2a8ae931a776366e
</oof:pre>

	<oof:header size="2">Network configuration</oof:header>
	<oof:p>
		SLASH2 uses the Lustre networking stack (aka LNet) so configuration
		will be somewhat familiar to those who use Lustre.
		At this time SLASH2 supports TCP and Infiniband sockets direct
		(SDP).
		Mixed network topologies are supported to some degree.
		For instance, if clients and I/O servers have Infiniband and
		Ethernet they may use IB for communication even if the MDS has only
		an Ethernet link.
	</oof:p>

	<oof:header size="2">Setting up the master configuration file</oof:header>
	<oof:p>
		Example configurations are provided in
		<oof:tt>projects/slash_nara/config</oof:tt>.
		The <ref sect='5'>slcfg</ref> man page also contains more
		information on this topic.
	</oof:p>

	<oof:header size="3">Setting zpool, fsuuid and network globals</oof:header>
	<oof:p>
		The previous two steps will provide the parameters for the
		<oof:tt>fsuuid</oof:tt>, <oof:tt>port</oof:tt>, and
		<oof:tt>nets</oof:tt> attributes.
		As configured here, the port used by the file system's TCP
		connections will be 989 (non-priviledged ports are allowed).
		The LNet network identifier for the TCP network is
		<oof:tt>tcp1</oof:tt>.
		Any clients or servers with interfaces on the 192.168 network will
		match the rule '192.168.*.*' and be configured on the
		<oof:tt>tcp1</oof:tt> network.
		Hosts with infininband interfaces on the 10.0.0.* network will be
		configured on the <oof:tt>sdp0</oof:tt> network.
	</oof:p>
	<oof:pre>
<oof:span class='syn_keyword'>set</oof:span> zpool_name=<oof:span class='syn_val'>"s2mds_pool"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> fsuuid=<oof:span class='syn_val'>"2a8ae931a776366e"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> port=<oof:span class='syn_val'>989</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> nets=<oof:span class='syn_val'>"tcp1 192.168.*.*; sdp0 10.0.0.*"</oof:span>;
</oof:pre>

	<oof:header size="3">Configuring a SLASH2 site</oof:header>
	<oof:p>
		A site is considered to be a management domain in the cloud or
		wide-area.
		<ref sect='5'>slcfg</ref> details the resource types and the example
		configurations in the distribution may be used as guides.
		Here we'll do a walk through of a simple site configuration.
	</oof:p>
	<oof:pre>
<oof:span class='syn_keyword'>set</oof:span> zpool_name=<oof:span class='syn_val'>"s2mds_pool"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> fsuuid=<oof:span class='syn_val'>"2a8ae931a776366e"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> port=989;
<oof:span class='syn_keyword'>set</oof:span> nets=<oof:span class='syn_val'>"tcp1 192.168.*.*; sdp0 10.0.0.*"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> pref_mds=<oof:span class='syn_val'>"mds1@MYSITE"</oof:span>;
<oof:span class='syn_keyword'>set</oof:span> pref_ios=<oof:span class='syn_val'>"ion1@MYSITE"</oof:span>;

<oof:span class='syn_keyword'>site</oof:span> @MYSITE {
     <oof:span class='syn_keyword'>site_desc</oof:span> = "test SLASH2 site configuration";
     <oof:span class='syn_keyword'>site_id</oof:span>   = 1;

     <oof:span class='syn_comment'># MDS resource</oof:span> #
     <oof:span class='syn_keyword'>resource</oof:span> mds1 {
	     <oof:span class='syn_keyword'>desc</oof:span> = <oof:span class='syn_val'>"my metadata server"</oof:span>;
	     <oof:span class='syn_keyword'>type</oof:span> = mds;
	     <oof:span class='syn_keyword'>id</oof:span>   = 0;
	     <oof:span class='syn_comment'># 'nids' should be the IP or hostname of your MDS node.</oof:span> #
	     <oof:span class='syn_comment'># It should be on the network specified in the variable 'nets'</oof:span> #
	     <oof:span class='syn_comment'># variable above.</oof:span> #
	     <oof:span class='syn_keyword'>nids</oof:span> = 192.168.0.100;
	     <oof:span class='syn_comment'># 'jrnldev' matches the device we formatted above.</oof:span> #
	     <oof:span class='syn_keyword'>jrnldev</oof:span> = /dev/sdJ1;
     }

     <oof:span class='syn_keyword'>resource</oof:span> ion1 {
	     <oof:span class='syn_keyword'>desc</oof:span> = <oof:span class='syn_val'>"I/O server 1"</oof:span>;
	     <oof:span class='syn_keyword'>type</oof:span> = standalone_fs;
	     <oof:span class='syn_keyword'>id</oof:span>   = 1;
	     <oof:span class='syn_keyword'>nids</oof:span> = 192.168.0.101, 10.0.0.1;
	     <oof:span class='syn_comment'># 'fsroot' points to the storage mounted on the I/O server</oof:span> #
	     <oof:span class='syn_comment'># which is to be used by SLASH2</oof:span> #
	     <oof:span class='syn_keyword'>fsroot</oof:span> = /disk;
     }

     <oof:span class='syn_keyword'>resource</oof:span> ion2 {
	   <oof:span class='syn_keyword'>desc</oof:span> = "I/O server 2";
	   <oof:span class='syn_keyword'>type</oof:span> = standalone_fs;
	   <oof:span class='syn_keyword'>id</oof:span>   = 2;
	   <oof:span class='syn_keyword'>nids</oof:span> = 192.168.0.102, 10.0.0.2;
	   <oof:span class='syn_keyword'>fsroot</oof:span> = /disk;
     }
}
</oof:pre>

	<oof:p>
		Note that the <oof:tt>pref_mds</oof:tt> and
		<oof:tt>pref_ios</oof:tt> global values have been filled in based on
		the names specified in the site.
		The <oof:tt>pref_ios</oof:tt> is important because it is used by
		clients as the default target when writing new files.
	</oof:p>

	<oof:header size="2">I/O Server Setup</oof:header>
	<oof:p>
		The configuration above lists two SLASH2 I/O servers, ion1 and ion2.
		Both of which list their <oof:tt>fsroot</oof:tt> as
		<oof:tt>/disk</oof:tt>.
		The SLASH2 I/O server is a stateless process which exports locally
		mounted storage into the respective SLASH2 file system.
		In this case, we assume that <oof:tt>/disk</oof:tt> is a mounted
		file system with some available storage behind it.
	</oof:p>
	<oof:p>
		In order to use the storage mounted at <oof:tt>/disk</oof:tt>, a
		specific directory structure must be created with the slmkfs command
		prior to starting the I/O server.
		The UUID of the file system must be supplied as a parameter.
		Per this example, the following command is run on both ion1 and
		ion2:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>io</oof:span><oof:span class='prompt_meta'>#</oof:span> slmkfs -i -u 0x2a8ae931a776366e /disk
<oof:span class='prompt_hostname'>io</oof:span><oof:span class='prompt_meta'>$</oof:span> ls -l /disk/.slmd/
total 4
drwx------ 3 root root 4096 Jun 18 15:10 2a8ae931a776366e
</oof:pre>

	<oof:p>
		Once completed, the directory <oof:tt>/disk/.slmd</oof:tt> should
		appear which contains a subdirectory named for theUUID.
		Under <oof:tt>/disk/.slmd</oof:tt> is a hierarchy of 16^4
		directories used for storing SLASH2 file objects.
	</oof:p>
	<oof:p>
		An explanation of more sophisticated I/O system types is given here:
		SLASH2IOServices
		RunningSLASH2Services
	</oof:p>
</xdc>
