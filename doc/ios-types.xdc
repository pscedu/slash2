<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>I/O system types</title>

	<oof:header size='1'>Overview</oof:header>
	<oof:p>
		This document describes the configuration types for I/O systems
		in a SLASH2 deployment.
		An I/O system in SLASH2 comprises a logical set of one or more
		storage nodes.
	</oof:p>
	<oof:p>
		For more information about each IOS type, consult the
		<ref sect='5'>slcfg</ref> manual.
	</oof:p>

	<oof:header size='1'>Standalone I/O system</oof:header>
	<oof:p>
		The standalone IOS is the most basic form of a SLASH2 I/O system:
	</oof:p>
	<oof:pre>
resource lemon {
	desc	= "Stand-alone I/O server";
	type	= standalone_fs;
	id	= 2;
	nids	= lemon.psc.edu;
	fsroot	= /local/s2io;
}
</oof:pre>

	<oof:p>
		The main parameter fsroot specifies the path to the underlying
		storage which is to be exported into the SLASH2 filesystem.
		The SLASH2 I/O daemon will expect to find its private internal
		directory hierarchy at this location.
		Currently, the SLASH2 I/O service sliod(8) operates on a single file
		system root.
	</oof:p>

	<oof:header size='1'>Archival I/O system</oof:header>
	<oof:p>
		The archival I/O type is used for systems which frequently encounter
		long delays on read such as a tape archiver or other type of jukebox
		system.
		The primary difference between archival and standalone is internal
		timeout mechanism for network requests.
		Otherwise, the configuration is the same:
	</oof:p>
	<oof:pre>
resource tapearchiver {
	desc	= "Archival I/O server";
	type	= archival_fs;
	id	= 3;
	nids	= turtle.psc.edu;
	fsroot	= /tape;
}
</oof:pre>

	<oof:header size='1'>Cluster "non shared" I/O system (CNOS)</oof:header>
	<oof:p>
		The CNOS I/O type is used for binding logical sets of standalone
		nodes into a "single" resource (for organizational purposes).
		For instance, 4 I/O nodes, each with local storage, may be bound
		into a "single" I/O resource for easier facilitation of striping and
		load balancing:
	</oof:p>
<oof:pre>
site PSC {
	resource ios0 {
		desc	= "I/O server 0";
		type	= standalone_fs;
		id	= 100;
		nids	= ios0.psc.edu;
		fsroot	= /local;
	}

	resource ios1 {
		desc	= "I/O server 1";
		type	= standalone_fs;
		id	= 101;
		nids	= ios1.psc.edu;
		fsroot	= /local;
	}

	resource ios2 {
		desc	= "I/O server 2";
		type	= standalone_fs;
		id	= 102;
		nids	= ios2.psc.edu;
		fsroot	= /local;
	}

	resource ios3 {
		desc	= "I/O server 3";
		type	= standalone_fs;
		id	= 103;
		nids	= ios3.psc.edu;
		fsroot	= /local;
	}

	resource ios_cnos {
		desc	= "cluster no share of ios0-3";
		type	= cluster_noshare_lfs;
		id	= 1000;
		ios	= ios0@PSC, ios1@PSC, ios2@PSC, ios3@PSC;
	}
	...
}
</oof:pre>

	<oof:p>
		When a client selects <oof:tt>ios_cnos@PSC</oof:tt> as its preferred
		I/O system, writes will be load balanced amongst the available
		members of the CNOS.
		Selection of the preferred I/O system is done via
		<ref sect='5'>slcfg</ref> configuration or during runtime via
		<ref sect='8'>msctl</ref>:
	</oof:p>
	<oof:pre>
# msctl -p pref_ios=ios_cnos@PSC
</oof:pre>
</xdc>
