<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Networking stack and configuration</title>

	<oof:header size="1">Overview</oof:header>
	<oof:p>
		This document describes the network stack initialization and
		<ref sect='5'>slcfg</ref> automatic profile self discovery.
	</oof:p>
	<oof:p>
		The following actions are taken depending on the value of the
		<oof:tt>nets</oof:tt> <ref sect='5'>slcfg</ref> option:
	</oof:p>
	<oof:list>
		<oof:list-item>
			if <oof:tt>nets</oof:tt> is a single Lustre network name, such as
			<oof:tt>tcp10</oof:tt>, network interface/Lustre network name
			pairs are constructed from each interface address registered in
			the system and this specified Lustre network name.
			The resulting list of pairs is copied into the daemon process
			environment as <oof:tt>LNET_NETWORKS</oof:tt>.
		</oof:list-item>
		<oof:list-item>
			if <oof:tt>nets</oof:tt> is a fully fledged
			<oof:tt>LNET_IP2NETS</oof:tt> value, this value is used to
			determine which addresses held by SLASH2 peers correspond to which
			Lustre networks and the system's network interfaces are queried to
			determine routes to such peers.
		</oof:list-item>
	</oof:list>
	<oof:p>
		These behaviors will be overridden if <oof:tt>LNET_NETWORKS</oof:tt>
		is already specified in the process environment, which always takes
		precedence.
	</oof:p>
	<oof:p>
		Otherwise, <oof:tt>LNET_NETWORKS</oof:tt> is fabricated based on the
		values specified in the SLASH2 configuration.
	</oof:p>
	<oof:p>
		<oof:tt>LNET_NETWORKS</oof:tt> is then parsed to determine routable
		addresses and their corresponding Lustre network names.
		These pairs are used to assign Lustre networks to the resource node
		addresses listed in the <ref sect='5'>slcfg</ref> site resources
		configuration.
	</oof:p>
</xdc>
