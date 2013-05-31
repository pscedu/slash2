<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Running SLASH2</title>

	<oof:header size="1">Running SLASH2</oof:header>

	<oof:header size="2">Overview</oof:header>
	<oof:p>
		To run an instance of SLASH2, you need to configure three types of
		services: metadata service (<ref sect='8'>slashd</ref>), I/O service
		(<ref sect='8'>sliod</ref>), and client service
		(<ref sect='8'>mount_slash</ref>).
		The <ref sect='7'>sladm</ref> manual provides a good overview of how
		to deploy each service type.
		Basically, there are three major steps to configure a SLASH2
		deployment:
	</oof:p>
	<oof:list>
		<oof:list-item>
			A metadata server (MDS) machine must be set up to run the SLASH2
			MDS service provided by the daemon
			<ref sect='8'>slashd</ref>.
			This service requires a ZFS pool (zpool) to store data.
			Before operation, the zpool must be formatted with
			<ref sect='8'>slmkfs</ref>.
			In addition, a journal file is needed for the MDS daemon to track
			on-going operations.
		</oof:list-item>
		<oof:list-item>
			At least one I/O node must be set up that will run
			<ref sect='8'>sliod</ref>.
			<oof:tt>sliod</oof:tt> can contribute any existing POSIX file
			system to the SLASH2 network it is a member of after formatting
			with <ref sect='8'>slmkfs</ref>.
			The formatting process simply creates a directory hierarchy that
			<oof:tt>sliod</oof:tt> will operate under.
		</oof:list-item>
		<oof:list-item>
			At least one client service must be set up that will run
			<ref sect='8'>mount_slash</ref>.
		</oof:list-item>
	</oof:list>

	<oof:header size="2">Starting the MDS</oof:header>
	<oof:p>
		Command-line flags, runtime files, environment variables, etc. are
		all described in <ref sect='8'>slashd</ref>.
		A wrapper script is provided in the distribution that monitors and
		restarts <oof:tt>slashd</oof:tt> upon failure and compiles and sends
		bug reports when problems arise in operation.
	</oof:p>
	<oof:p>
		Modify the file <oof:tt>pfl/utils/daemon/pfl_daemon.cfg</oof:tt>
		and add configuration sections for the deployment.
	</oof:p>
	<oof:p>
		Finally, launch the MDS:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> slashd.sh
</oof:pre>

	<oof:header size="3">Controlling <oof:tt>slashd</oof:tt></oof:header>
	<oof:p>
		<ref sect='8'>slmctl</ref> serves as the interface to the running
		<ref sect='8'>slashd</ref> process, providing administrators with
		numerous options to query parameters and control behavior.
		A simple peer connections query is a good way to test general
		service availibility:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>$</oof:span> slmctl -sc
resource                                     host type     flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_psc_ios7_0                  sense7.psc.edu serial   -OM-- 19149    8    0
  xwfs_tacc_0                       129.114.32.90 serial   -OM-- 19150    8    0
  xwfs_tacc_1                       129.114.32.91 serial   -OM-- 19150    8    0
  xwfs_tacc_2                       129.114.32.92 serial   -OM-- 19150    8    0
  xwfs_tacc_3                       129.114.32.93 serial   --M-- 19150    8    0
clients
  _                    firehose2.psc.teragrid.org          -O---     0    8    2
</oof:pre>

	<oof:p>
		The above command lists the clients and I/O servers connected to the
		MDS and also demonostrates the basic health of the MDS.
	</oof:p>

	<oof:header size="3">zfs and zpool</oof:header>
	<oof:p>
		Once the MDS is operational, the <ref sect='8'>zfs</ref> and
		<ref sect='8'>zpool</ref> commands may be used for any necessary
		pool adminstration:
	</oof:p>
<oof:pre>
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zfs get compressratio
NAME            PROPERTY       VALUE  SOURCE
xwfs_test       compressratio  3.28x  -
xwfs_test@test  compressratio  3.04x  -

<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zpool iostat
	       capacity     operations    bandwidth
pool        alloc   free   read  write   read  write
----------  -----  -----  -----  -----  -----  -----
xwfs_test   1001M   148G      3      0  7.69K  7.23K

<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zpool scrub xwfs_test
<oof:span class='prompt_hostname'>mds</oof:span><oof:span class='prompt_meta'>#</oof:span> zfs send xwfs_test@test &gt; /local/xwfs_test@test.zstream
</oof:pre>

	<oof:header size="2">Running the SLASH2 I/O service (<oof:tt>sliod</oof:tt>)</oof:header>
	<oof:p>
		Prior to starting <ref sect='8'>sliod</ref> on an I/O server, make
		sure the following items are available:
	</oof:p>
	<oof:list>
		<oof:list-item>
			the SLASH2 deployment configuration file, usually
			<oof:tt>/etc/slcfg</oof:tt>
		</oof:list-item>
		<oof:list-item>
			the SLASH2 deployment daemon list file, usually
			<oof:tt>/usr/local/pfl_daemon.cfg</oof:tt>
		</oof:list-item>
		<oof:list-item>
			the SLASH2 deployment secret communication key, usually
			<oof:tt>/var/lib/slash/authbuf.key</oof:tt>, which is
			automatically generated by the MDS or may be done so manually by
			<ref sect='8'>slkeymgt</ref>
		</oof:list-item>
		<oof:list-item>
			formatted storage area that will be contributed to the SLASH2
			deployment network.
			<oof:tt>sliod</oof:tt> can serve any POSIX file system to the
			SLASH2 network as long as the resource has been formatted with
			<ref sect='8'>slmkfs</ref>.
		</oof:list-item>
	</oof:list>
	<oof:p>
		Launch the I/O service:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>io</oof:span><oof:span class='prompt_meta'>#</oof:span> sliod.sh
</oof:pre>

	<oof:header size="3">Controlling <oof:tt>sliod</oof:tt></oof:header>
	<oof:p>
		Similar to <ref sect='8'>slmctl</ref>, <ref sect='8'>sliod</ref> has
		its own control interface program: <ref sect='8'>slictl</ref>.
		A similar basic <oof:tt>sliod</oof:tt> daemon health and connection
		status inquiry may be performed:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>io</oof:span><oof:span class='prompt_meta'>$</oof:span> slictl -sc
resource                                    host type      flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_mds                         citron.psc.edu mds      -O--P 19111    8    0
  xwfs_tacc_0                       129.114.32.90 serial   -----     0    0    0
  xwfs_tacc_1                       129.114.32.91 serial   -----     0    0    0
  xwfs_tacc_2                       129.114.32.92 serial   -----     0    0    0
  xwfs_tacc_3                       129.114.32.93 serial   -----     0    0    0
</oof:pre>

	<oof:header size="2">Running the client service <oof:tt>mount_slash</oof:tt></oof:header>
	<oof:p>
		Identical to the <ref sect='8'>sliod</ref> prerequisites, prior to
		starting <ref sect='8'>mount_slash</ref> on a client machine, make
		sure the following items are available:
	</oof:p>
	<oof:list>
		<oof:list-item>
			the SLASH2 deployment configuration file, usually
			<oof:tt>/etc/slcfg</oof:tt>
		</oof:list-item>
		<oof:list-item>
			the SLASH2 deployment daemon list file, usually
			<oof:tt>/usr/local/pfl_daemon.cfg</oof:tt>
		</oof:list-item>
		<oof:list-item>
			the SLASH2 deployment secret communication key, usually
			<oof:tt>/var/lib/slash/authbuf.key</oof:tt>
		</oof:list-item>
		<oof:list-item>
			the attachment mount point (e.g. <oof:tt>mkdir /s2</oof:tt>)
		</oof:list-item>
	</oof:list>

	<oof:p>
		Launch the client service:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>client</oof:span><oof:span class='prompt_meta'>#</oof:span> mount_slash.sh
</oof:pre>

	<oof:header size="3">Controlling <oof:tt>mount_slash</oof:tt></oof:header>
	<oof:p>
		Like <ref sect='8'>slmctl</ref> and <ref sect='8'>slictl</ref>, an
		analogous command <ref sect='8'>msctl</ref> is provided to control
		live operation of <ref sect='8'>mount_slash</ref>.
		Again, a connection status request can be used to check connectivity
		to peers and basic client service health:
	</oof:p>
	<oof:pre>
<oof:span class='prompt_hostname'>client</oof:span><oof:span class='prompt_meta'>$</oof:span> msctl -sc
resource                                    host type      flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_mds                         citron.psc.edu mds      -O--- 19111    8  601
  xwfs_psc_ios7_0                  sense7.psc.edu serial   ----- 19149    8    0
  xwfs_tacc_0                       129.114.32.90 serial   -O--- 19150    8    0
  xwfs_tacc_1                       129.114.32.91 serial   -O--- 19150    8    0
  xwfs_tacc_2                       129.114.32.92 serial   -O--- 19150    8    0
  xwfs_tacc_3                       129.114.32.93 serial   -O--- 19150    8    0
</oof:pre>
</xdc>
