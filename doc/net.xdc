<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Configuring a SLASH2 deployment</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		This document describes the network stack initialization and
		slash.slcfg automatic profile self discovery.
	</oof:p>
	<oof:p>
		The following actions are taken depending on the value of the 'nets'
		slash.slcfg option:
	</oof:p>
	<oof:list>
		<oof:list-item>
			if 'nets' is a single Lustre network name, such as "tcp10",
			network interface/Lustre network name pairs are constructed from
			each interface address registered in the system and this specified
			Lustre network name.
			The resulting list of pairs is copied into the daemon process
			environment as LNET_NETWORKS.
		</oof:list-item>
		<oof:list-item>
			if 'nets' is a fully fledged LNET_IP2NETS value, this value is
			used to determine which addresses held by SLASH2 peers correspond
			to which Lustre networks and the system's network interfaces are
			queried to determine routes to such peers.
		</oof:list-item>
	</oof:list>
	<oof:p>
		These behaviors will be overridden if LNET_NETWORKS is already
		specified in the process environment, which always takes precedence.
	</oof:p>
	<oof:p>
		Otherwise, LNET_NETWORKS is fabricated based on the values specified
		in the SLASH2 configuration.
	</oof:p>
	<oof:p>
		LNET_NETWORKS is then parsed to determine routable addresses and
		their corresponding Lustre network names.
		These pairs are used to assign Lustre networks to the resource node
		addresses listed in the slash.slcfg site resources configuration.
	</oof:p>
</xdc>
