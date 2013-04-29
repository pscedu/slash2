<?xml version="1.0" ?>
<!-- $Id$ -->

<xdc>
	<title>Configuring a SLASH2 deployment</title>

	<h1>Authenticate models</h1>
	<h2>Phase 1 - User ID mapping</h2>
	<p>
		At this time, interoperation between sites is not possible.
		With relative ease, a UID translator can be placed at mount_slash
		endpoints which translates UIDs from the OS to the global SLASH2 UID
		when handling syscalls and vice versa when replying to syscalls.
	</p>

	<h2>Phase 2 - Host based authentication</h2>
	<p>
		However, with no root squashing, and even if we add it, there is
		still the problem of compromised/untrusted mount_slash endpoints
		having full reign over all data in the SLASH2 file system.
		Host-based authentication solves some of these problems in that it
		can provide a mechanism to revoke access to the SLASH2 file system
		in cases of known compromised mount_slash endpoints.
	</p>
	<p>
		Furthermore, restrictions can be placed on the certificates for
		mount_slash endpoints such that all accesses will only be granted to
		certain UIDs.
	</p>
	<p>
		However, this model still does not solve the problem where an
		unknown compromised mount_slash with full reign privileges (that is,
		no UID restrictions; for example, on a large compute system) can
		wreak havoc on other users' files.
	</p>
	<p>
		This solution entails deploying a PKI model to each host on the
		network.
		This means generating a certificate for each slashd, sliod, and
		mount_slash that wishes to participate on the SLASH2 network.
	</p>

	<h2>Phase 3 - Full Kerberos support</h2>
	<p>
		This approach alleviates all problems with the previous approaches
		at the cost of implementation and deployment complexity.
		But even in the case of unknown compromised full access mount_slash
		endpoints, only files owned by accounts with valid cached Kerberos
		tickets on the endpoint may be accessed.
	</p>
	<p>
		Idea from Eric Barton: that encryption/uid mapping take place at or
		close to the router level or on the router.
	</p>

	<h3>slauthd</h3>
	<ul>
		<li>runs on clients as part of mount_slash</li>
		<li>
			handles authentication using kerberos
			<ul>
				<li>user is who he says</li>
				<li>user is allowed to setup time limited grants for clients</li>
			</ul>
		</li>
		<li>
			handles grant relay to slash server authd and/or slash mds
			<ul>
				<li>grant user@domain(s) access to slash uid for time period</li>
				<li>revoke grants</li>
			</ul>
		</li>
		<li>
			*possible* vouches for user from hosts with grants.
			aka slaccd contacts a slauthd client for kerberos verification.
		</li>
	</ul>

	<h3>slaccd</h3>
	<ul>
		<li>runs as server on slash mds or seperate server</li>
		<li>maintains access grants</li>
		<li>is queried to determine if client reads/writes should be processed</li>
		<li>maintains keys for data encryption</li>
		<li>may be coupled with a slash kerberos deployment</li>
	</ul>

	<h3>Example</h3>
	<ul>
		<li>assume a kerb realm "SITE.ORG"</li>
		<li>assume a compute cluster "cluster.org" with nodes
			"node1.cluster.org", "node2.cluster.org", etc.</li>
		<li>assume a client machine "client.org"</li>
	</ul>

	<p>Steps a user would take to use the file system:</p>
	<ol>
		<li>user on client.org runs slauth_init (random example below)
			<pre>
$ slauth_init rbudden@SITE.ORG --slashuser=rmbudden --grant="node*.cluster.org" --time=24h
</pre></li>
		<li>user enters his slash krb password</li>
		<li>kerberos is contacted and user is given a ticket</li>
		<li>the ticket is sent to slauthd which tells slauthd that the user
			is allowed to access the file system from the local client and
			that the user is allowed to setup grants for access</li>
		<li>slauthd approves grants for user on "node1.cluster.org,
			node2.cluster.org, etc."</li>
		<li>grants are sent to slaccd (slaccd knows user is allowed file
			system access from node*.cluster.org for 24 hours using the
			username rmbudden [not necessarily the same slash krb principle
			rbudden])</li>
		<li>user submits computation job</li>
		<li>computation job tries to write to slash</li>
		<li>slash mount asks slaccd if it's permitted to write to the file
			system</li>
		<li>slaccd looks up grants for user and approves I/O on the node for
			specified time (insert possible slauthd contact to obtain kerberos
			credentials)</li>
		<li>approval is cached on the client mount</li>
		<li>I/O proceeds accordingly</li>
	</ol>

	<p>Other ideas:</p>
	<ul>
		<li>Having a way to request auth from a client, say in the job
			script, then revoke auth after the job has finished.</li>
	</ul>

	<p>This might be for data encryption such as:</p>
	<pre>$ slauth_request_key</pre>
	<p>(slauthd approves grant, issues a new encryption key)<br />
		...<br />
		job runs<br />
		...</p>
	<pre>$ slauth_revoke_key</pre>
	<p>(slauthd revokes encryption key)</p>
	<pre>$ slauth_revoke_grant</pre>
	<p>(slauthd revokes access grant from computational nodes since job is
		finished)</p>

</xdc>
