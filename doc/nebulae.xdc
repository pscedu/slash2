<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Nebulae Network Scheduling Engine</title>

	Design

		site A {
			resource md  { }
			resource io0 { }		# IB to io1, 10gE to io2
			resource io1 { }		# IB to io0, 10gE to io2
			resource io2 { }		# 10gE to io[01]
			resource mem { }		#
		}
													# A <- 10gE link -> B
													# A <-  1gE link -> B
													# A <-  1gE link -> B
													# A <-  1gE link -> B
													# A <-  1gE link -> B
		site B {
			resource io0 { }		# 8xIB to io1, 4xIB to io2
			resource io1 { }		# 8xIB to io0, 2xIB to io2
			resource io2 { }		# 4xIB to io0, 2xIB to io1
			resource mem { }		#
		}

	All SLASH2 peers in the configuration are graph vertices.
	When performing replication arrangements, all edges connecting the
	tentative source and destination IOSes must have reservations made.

	This structure must be serialized and transmitted to other MDSes.

		/* smallest allocatable unit */
		#define RESMLINK_UNITSZ 4096

		struct slm_resmlink {
			sl_ios_id_t		 srl_src;
			sl_ios_id_t		 srl_dst;
			int32_t				 srl_avail;		/* units of RESMLINK_UNITSZ/sec */
			int32_t				 srl_used;
		};

	This structure needs to allow peers to appear and disappear.

	Dynamically identifying networking characteristics.

	Metrics need to be recorded during replication and reported to local
	MDS when replication success/failure are reported.

	Detecting rate adjustments

	The MDS has an expectation of rates that should be achieved when
	arranging a replication.  When the report comes in, the acheived
	speeds are compared to the expected speeds and available bandwidth is
	adjusted accordingly.

	Applying this knowledge throughout the graph edge path is harder, as
	bottlenecks.

	We assume the MDS is sole proprietor of networking decisions, or at
	least within any static configuration given.




</xdc>
