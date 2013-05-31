<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>SLASH2 configuration example</title>

SLASH2 I/O Service Types

See the slfcg(5) manual page for more details.
Stand Alone I/O System (SAIOS)

The stand alone IOS is the most basic form of a SLASH2 I/O service and
is described in the following manner.

resource lemon {
    desc   = "Stand-alone I/O server";
    type   = standalone_fs;
    id   = 2;
    nids   = lemon.psc.edu;
    fsroot   = /local/s2io;
}

The main parameter fsroot specifies the path to the underlying storage
which is to be exported into the SLASH2 filesystem. The SLASH2 I/O
daemon will expect to find its private internal directory hierarchy at
this location. Currently, the SLASH2 I/O service sliod(8) operates on a
single file system root.

Archival I/O System (AIOS)

The archival I/O type is used for systems which frequently encounter
long delays on read such as a tape archiver or other type of jukebox
system. The primary difference between AIOS and SAIOS is internal
timeout mechanism for network requests. Otherwise, the configuration is
the same:

resource tapearchiver {
    desc     = "Archival I/O server";
    type     = archival_fs;
    id       = 3;
    nids    = turtle.psc.edu;
    fsroot   = /tape;
}

Cluster No Share I/O System (CNOS)

The CNOS I/O type is used for binding logical sets of SAIOS nodes into a
"single" resource (for organizational purposes). For instance, 4 I/O
nodes, each with local storage, may be bound into a "single" I/O
resource for the purposes of striping and load balancing:

site PSC {
    resource ios0 {
       desc   = "I/O server 0";
       type   = standalone_fs;
       id   = 100;
       nids   = ios0.psc.edu;
       fsroot   = /local;
   }

    resource ios1 {
       desc   = "I/O server 1";
       type   = standalone_fs;
       id   = 101;
       nids   = ios1.psc.edu;
       fsroot   = /local;
   }

    resource ios2 {
       desc   = "I/O server 2";
       type   = standalone_fs;
       id   = 102;
       nids   = ios2.psc.edu;
       fsroot   = /local;
   }

    resource ios3 {
       desc   = "I/O server 3";
       type   = standalone_fs;
       id   = 103;
       nids   = ios3.psc.edu;
       fsroot   = /local;
   }

   resource ios_cnos {
       desc   = "cluster no share of ios0-3";
       type   = cluster_noshare_lfs;
       id   = 1000;
       ios   = ios0@PSC, ios1@PSC, ios2@PSC, ios3@PSC;
   }
   ...
}

When a client selects ios_cnos@PSC as its preferred I/O system, writes
will be load balanced amongst the available members of the CNOS.
Selection of the preferred I/O system is done via the configuration or
during runtime via msctl like so:

# msctl -p pref_ios=ios_cnos@PSC
</xdc>
