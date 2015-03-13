<?xml version="1.0" ?>
<!DOCTYPE xdc PUBLIC "-//PSC//XDC//EN" "http://www.psc.edu/~yanovich/xml/xdc.dtd">
<!-- $Id$ -->

<xdc xmlns:oof="http://www.psc.edu/~yanovich/xsl/oof-1.0">
	<title>SLASH2 Software Installation</title>

	<oof:header size="1">SLASH2 Software Installation</oof:header>

	<oof:header size="2">Overview</oof:header>

	<oof:p>
		This document will refer to subdirectories relative to the root of
		the SLASH2 top-level source code directory.
	</oof:p>

	<oof:p>
		Local customization may be necessary to adapt the code base for a
		specific machine/deployment environment.
		Such global customizations can be added to
		<oof:tt>mk/local.mk</oof:tt>:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> cat mk/local.mk
DEVELPATHS=0			# disable developer knobs
INST_BASE=/usr/local		# defaults to /usr/psc
</oof:pre>

	<oof:p>
		By default, <oof:tt>INST_BASE</oof:tt> defaults to
		<oof:tt>/usr/psc</oof:tt>, so it may be desirable to add
		<oof:tt>/usr/psc/bin</oof:tt> and <oof:tt>/usr/psc/sbin</oof:tt> to
		your <oof:tt>$PATH</oof:tt> and <oof:tt>/usr/psc/man</oof:tt> to
		your <oof:tt>$MANPATH</oof:tt>, etc.
	</oof:p>

	<oof:p>
		Next, select which components of the SLASH2 software will be
		necessary for your deployment.
		By default, all of the MDS, I/O, and client components are built.
		If any of these components is not necessary, you can create local
		settings to override the build process by specifying only which
		components should be built in
		<oof:tt>slash2/mk/local.mk</oof:tt>.
	</oof:p>
	<oof:p>
		For example, to disable building of the MDS, specify only
		<oof:tt>ion</oof:tt> and <oof:tt>cli</oof:tt> in
		<oof:tt>local.mk</oof:tt>:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> echo "SLASH_MODULES= ion cli" &gt; slash2/mk/local.mk
</oof:pre>
	<oof:p>
		Also, to enable the installer to pick up the SLASH2 configuration
		file:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> echo "SLCFGV=config/<oof:strong>$mysite.slcfg</oof:strong>:<oof:strong>/usr/local/my/inst/dir</oof:strong>" &gt; slash2/mk/local.mk
</oof:pre>

	<oof:header size="2">Compilation Prerequisites</oof:header>

	<oof:header size="3">Required by all components</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>
			<oof:link href='http://www.gnu.org/software/make'>GNU make</oof:link></oof:list-item>
		<oof:list-item>
			<oof:link href='http://directory.fsf.org/project/libgcrypt/'>libgcrypt</oof:link></oof:list-item>
		<oof:list-item>pkg-config</oof:list-item>
		<oof:list-item>yacc (Berekely yacc is known to work)</oof:list-item>
		<oof:list-item>lex (flex is known to work)</oof:list-item>
	</oof:list>

	<oof:header size="4">MDS-specific</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>libaio is required by zfs-fuse.</oof:list-item>
		<oof:list-item>OpenSSL is required by zfs-fuse.</oof:list-item>
		<oof:list-item>
			<oof:link href='http://www.scons.org/'>scons</oof:link> is
			required to build zfs-fuse.
		</oof:list-item>
		<oof:list-item>sqlite3 is required.</oof:list-item>
		<oof:list-item>
			<oof:link href='http://zfs-fuse.net/'>zfs-fuse</oof:link>
			is packaged within our repository and is built through our
			framework when the MDS component is enabled.
			</oof:list-item>
	</oof:list>

	<oof:header size="4">Client-specific</oof:header>
	<oof:list type="LIST_UN">
		<oof:list-item>ncurses</oof:list-item>
		<oof:list-item>
			<oof:link href='http://fuse.sourceforge.net/'>FUSE</oof:link> is
			also packaged within our repository and can be built with:
			<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> cd distrib/fuse
<oof:span class='prompt_meta'>$</oof:span> ./configure
<oof:span class='prompt_meta'>$</oof:span> make
<oof:span class='prompt_meta'>#</oof:span> make install
</oof:pre>

			<oof:p>
				If it is not possible to install our packaged version of FUSE, you
				may reuse the one available on your system.  Just specify the
				location of <oof:tt>fuse.pc</oof:tt> in
				<oof:tt>$PROJ_ROOT/mk/local.mk</oof:tt>:
			</oof:p>
			<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> echo 'PKG_CONFIG_PATH=/usr/local/fuse' >> mk/local.mk
</oof:pre>
			<oof:p>
				We package a version to attempt to reduce potential bugs in
				various versions and patches in the wild, but it should not be
				strictly necessary to use ours.
			</oof:p>
		</oof:list-item>
	</oof:list>

	<oof:header size="4">Building instructions</oof:header>

	<oof:p>
		Finally, build the SLASH2 software with <oof:tt>gmake</oof:tt>:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> make build
<oof:span class='prompt_meta'>#</oof:span> make install
</oof:pre>

	<oof:p>
		Note: SLASH2 actually requires <oof:tt>gmake</oof:tt> to build, so
		make sure this command is specifically used on systems such as BSD
		that don't default to GNU make.
	</oof:p>

	<oof:p>
		If the build finished without errors, congratulations on compiling
		the SLASH2 system source code!
		All files will install into <oof:tt>/usr/psc</oof:tt>.
		Administrators may now proceed with a SLASH2 deployment by
		consulting the <oof:link href='/mdoc.pwl?q=sladm;sect=7'>configuration
		and deployment documentation</oof:link>.
	</oof:p>

	<oof:p>
		If errors were encountered, feel free to gripe, providing error
		details and operating system information and version, to the
		<oof:link href='/contact.pwl#mlist'>development mailing
			list</oof:link>.
	</oof:p>

	<oof:header size="4">System reconfiguration</oof:header>
	<oof:p>
		SLASH2 uses a compatibility discovery system similar to
		<oof:tt>autoconf</oof:tt> and the <oof:tt>./configure</oof:tt>
		scripts offered by many common open source software projects.
		This configuration is automatically generated before the first
		compilation and regenerated subsequently based on heuristics when
		something on the build host has changed.
	</oof:p>
	<oof:p>
		In the case of missing system packages, or updated system packages,
		etc. when it is necessary to reperform this configuration probe
		(such as when <oof:tt>./configure</oof:tt> would need to be rerun),
		this configuration can be cleared so that it will be redone by
		removing the file
		<oof:tt>mk/gen-localdefs-${hostname}-pickle.mk</oof:tt>.
	</oof:p>

	<oof:header size="3">Recompiling/Upgrading</oof:header>

	<oof:p>
		Updating a SLASH2 checkout via Subversion must be executed from the
		top-level projects directory to ensure any changes made in common
		libraries outside the SLASH2 source root are also fetched:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> make up
</oof:pre>

	<oof:p>
		After source modifications have been made, whether manually or after
		pulling updates, recompile the suite:
	</oof:p>
	<oof:pre class='code'>
<oof:span class='prompt_meta'>$</oof:span> make build
<oof:span class='prompt_meta'>#</oof:span> make install
</oof:pre>

</xdc>
