<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc
	xmlns="http://www.psc.edu/~yanovich/xsl/xdc-1.0"
	xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>Authentication models</title>

	<oof:header size="1">Authenticate models</oof:header>
	<oof:header size="2">Phase 1 - User ID mapping</oof:header>
	<oof:p>
		At this time, interoperation between sites is not possible.
		With relative ease, a UID translator can be placed at mount_slash
		endpoints which translates UIDs from the OS to the global SLASH2 UID
		when handling syscalls and vice versa when replying to syscalls.
	</oof:p>

	<oof:header size="2">Phase 2 - Host based authentication</oof:header>
	<oof:p>
		However, with no root squashing, and even if we add it, there is
		still the problem of compromised/untrusted mount_slash endpoints
		having full reign over all data in the SLASH2 file system.
		Host-based authentication solves some of these problems in that it
		can provide a mechanism to revoke access to the SLASH2 file system
		in cases of known compromised mount_slash endpoints.
	</oof:p>
	<oof:p>
		Furthermore, restrictions can be placed on the certificates for
		mount_slash endpoints such that all accesses will only be granted to
		certain UIDs.
	</oof:p>
	<oof:p>
		However, this model still does not solve the problem where an
		unknown compromised mount_slash with full reign privileges (that is,
		no UID restrictions; for example, on a large compute system) can
		wreak havoc on other users' files.
	</oof:p>
	<oof:p>
		This solution entails deploying a PKI model to each host on the
		network.
		This means generating a certificate for each slashd, sliod, and
		mount_slash that wishes to participate on the SLASH2 network.
	</oof:p>

	<oof:header size="2">Phase 3 - Full Kerberos support</oof:header>
	<oof:p>
		This approach alleviates all problems with the previous approaches
		at the cost of implementation and deployment complexity.
		But even in the case of unknown compromised full access mount_slash
		endpoints, only files owned by accounts with valid cached Kerberos
		tickets on the endpoint may be accessed.
	</oof:p>
	<oof:p>
		Idea from Eric Barton: that encryption/uid mapping take place at or
		close to the router level or on the router.
	</oof:p>

	<oof:header size="3">slauthd</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>runs on clients as part of mount_slash</oof:list-item>
		<oof:list-item>
			handles authentication using kerberos
			<oof:list type="LIST_UN">
				<oof:list-item>user is who he says</oof:list-item>
				<oof:list-item>user is allowed to setup time limited grants for
					clients</oof:list-item>
			</oof:list>
		</oof:list-item>
		<oof:list-item>
			handles grant relay to slash server authd and/or slash mds
			<oof:list type="LIST_UN">
				<oof:list-item>grant user@domain(s) access to slash uid for time
					period</oof:list-item>
				<oof:list-item>revoke grants</oof:list-item>
			</oof:list>
		</oof:list-item>
		<oof:list-item>
			*possible* vouches for user from hosts with grants.
			aka slaccd contacts a slauthd client for kerberos verification.
		</oof:list-item>
	</oof:list>

	<oof:header size="3">slaccd</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>runs as server on slash mds or seperate server</oof:list-item>
		<oof:list-item>maintains access grants</oof:list-item>
		<oof:list-item>is queried to determine if client reads/writes should
			be processed</oof:list-item>
		<oof:list-item>maintains keys for data encryption</oof:list-item>
		<oof:list-item>may be coupled with a slash kerberos deployment</oof:list-item>
	</oof:list>

	<oof:header size="3">Example</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>assume a kerb realm "SITE.ORG"</oof:list-item>
		<oof:list-item>assume a compute cluster "cluster.org" with nodes
			"node1.cluster.org", "node2.cluster.org", etc.</oof:list-item>
		<oof:list-item>assume a client machine "client.org"</oof:list-item>
	</oof:list>

	<oof:p>Steps a user would take to use the file system:</oof:p>
	<ol>
		<oof:list-item>user on client.org runs slauth_init (random example below)
			<oof:pre>
$ slauth_init rbudden@SITE.ORG --slashuser=rmbudden --grant="node*.cluster.org" --time=24h
</oof:pre></oof:list-item>
		<oof:list-item>user enters his slash krb password</oof:list-item>
		<oof:list-item>kerberos is contacted and user is given a ticket</oof:list-item>
		<oof:list-item>the ticket is sent to slauthd which tells slauthd that the user
			is allowed to access the file system from the local client and
			that the user is allowed to setup grants for access</oof:list-item>
		<oof:list-item>slauthd approves grants for user on "node1.cluster.org,
			node2.cluster.org, etc."</oof:list-item>
		<oof:list-item>grants are sent to slaccd (slaccd knows user is allowed file
			system access from node*.cluster.org for 24 hours using the
			username rmbudden [not necessarily the same slash krb principle
			rbudden])</oof:list-item>
		<oof:list-item>user submits computation job</oof:list-item>
		<oof:list-item>computation job tries to write to slash</oof:list-item>
		<oof:list-item>slash mount asks slaccd if it's permitted to write to the file
			system</oof:list-item>
		<oof:list-item>slaccd looks up grants for user and approves I/O on the node for
			specified time (insert possible slauthd contact to obtain kerberos
			credentials)</oof:list-item>
		<oof:list-item>approval is cached on the client mount</oof:list-item>
		<oof:list-item>I/O proceeds accordingly</oof:list-item>
	</ol>

	<oof:p>Other ideas:</oof:p>
	<oof:list type="LIST_UN">
		<oof:list-item>Having a way to request auth from a client, say in the job
			script, then revoke auth after the job has finished.</oof:list-item>
	</oof:list>

	<oof:p>This might be for data encryption such as:</oof:p>
	<oof:pre>$ slauth_request_key</oof:pre>
	<oof:p>(slauthd approves grant, issues a new encryption key)<oof:br />
		...<oof:br />
		job runs<oof:br />
		...</oof:p>
	<oof:pre>$ slauth_revoke_key</oof:pre>
	<oof:p>(slauthd revokes encryption key)</oof:p>
	<oof:pre>$ slauth_revoke_grant</oof:pre>
	<oof:p>(slauthd revokes access grant from computational nodes since job is
		finished)</oof:p>

</xdc>
