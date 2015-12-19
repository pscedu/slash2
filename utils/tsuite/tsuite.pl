#!/usr/bin/perl -W
# $Id$
# %GPL_START_LICENSE%
# ---------------------------------------------------------------------
# Copyright 2015, Google, Inc.
# Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License contained in the file
# `COPYING-GPL' at the top of this distribution or at
# https://www.gnu.org/licenses/gpl-2.0.html for more details.
# ---------------------------------------------------------------------
# %END_LICENSE%

use File::Basename;
use Getopt::Std;
use POSIX qw(:sys_wait_h :errno_h);
use IPC::Open3;
use Net::SMTP;
use Cwd;
use strict;
use warnings;

my $TSUITE_REL_BASE = 'slash2/utils/tsuite';
my $TSUITE_REL_FN = "$TSUITE_REL_BASE/tsuite.pl";

my %gcfg;	# suite's global configuration settings
my %opts;	# runtime options
my @mds;	# suite MDS nodes
my @ios;	# suite IOS nodes
my @cli;	# suite CLI nodes

getopts("mRv", \%opts) or usage();
usage() if @ARGV > 1;

my $ts_dir = dirname($0);
my $ts_name = "suite0";
$ts_name = $ARGV[0] if @ARGV > 0;

# Low level utility routines.
sub fatalx {
	die "$0: @_";
}

sub fatal {
	die "$0: @_: $!";
}

sub usage {
	die <<EOF
usage: $0 [-mRv] [test-name]

options:
  -m	send e-mail report
  -R	record results to database
  -v	verbose (debugging) output

EOF
}

sub init_env {
	my $n = shift;

	return <<EOF;
	set -e
	@{[ $opts{v} ? "set -x" : "" ]}
	@{[ map { "export $_='$n->{env}{$_}'\n" } keys $n->{env} ]}
	cd $n->{src_dir}
EOF
}

sub debug_msg {
	print WR @_, "\n" if $opts{v};
}

sub execute {
	debug_msg "executing: @_";
	system @_ or fatal @_;
}

sub slurp {
	my ($fn) = @_;
	local $/;

	open F, "<", "$fn" or fatal "open $fn";
	my $data = <F>;
	close F;
	return ($data);
}

use constant NONBLOCK => 1;

sub waitjobs {
	my ($pids, $to, $flags) = @_;
	local $_;

	alarm $to;
	for my $pid (@$pids) {
		my $status = waitpid $pid, 0;
		fatal "waitpid" if $status == -1;
		fatalx "child process exited nonzero: " . ($? >> 8) if $?;
	}
	alarm 0;

	@$pids = ();
}

sub runcmd {
	my ($cmd, $in) = @_;

	my $infh;
	my $pid = open3($infh, ">&WR", ">&WR", $cmd);
	print $infh $in;
	close $infh;

	return $pid;
}

sub mkdirs {
	my ($dir) = @_;
	my @cpn = split m!/!, $dir;
	my $fn = "";
	while (@cpn) {
		my $cpn = shift @cpn;
		next if $cpn eq "";
		$fn .= "/$cpn";
		unless (mkdir $fn) {
			return 0 unless $! == EEXIST;
		}
	}
	return 1;
}

sub push_vectorize {
	my ($n, $k, $v) = @_;
	if (exists $n->{$k}) {
		if (ref $n->{$k} eq "ARRAY") {
			push @{ $n->{$k} }, $v;
		} else {
			$n->{$k} = [ $n->{$k}, $v ];
		}
	} else {
		$n->{$k} = $v;
	}
}

# High level (application-level) utility routines.

# Parse configuration for MDS and IOS resources
sub parse_conf {
	my $ts_cfg = shift;

	my $in_site = 0;
	my $site_name;
	my @lines = split /\n/, $ts_cfg;
	my $cfg = \%gcfg;
	my $r;

	for (my $ln = 0; $ln < @lines; $ln++) {
		my $line = $lines[$ln];

		if ($line =~ /^\s*#\s*\@(\w+)\s+(.*)$/) {
			debug_msg "parsed parameter: $1=$2";
			push_vectorize($cfg, $1, $2);
		} elsif ($in_site) {
			if ($r) {
				if ($line =~ /^\s*type\s*=\s*(\S+)\s*;\s*$/) {
					$r->{type} = $1;
					if ($r->{type} eq "mds") {
						push @mds, $r;
					} else {
						push @ios, $r;
					}
				} elsif ($line =~ /^\s*id\s*=\s*(\d+)\s*;\s*$/) {
					$r->{id} = $1;
				} elsif ($line =~ /^\s*fsroot\s*=\s*(\S+)\s*;\s*$/) {
					($r->{fsroot} = $1) =~ s/^"|"$//g;
				} elsif ($line =~ /^\s*nids\s*=\s*(.*)$/) {
					my $tmp = $1;

					for (; ($line = $lines[$ln]) !~ /;/; $ln++) {
						$tmp .= $line;
					}
					$tmp =~ s/;\s*$//;
					$r->{host} = (split /\s*,\s*/, $tmp, 1)[0];
				} elsif ($line =~ /^\s*}\s*$/) {
					res_done($r);
					$r = undef;
				}
			} else {
				if ($line =~ /^\s*resource\s+(\w+)\s*{\s*$/) {
					$r = {
						res_name => $1,
						site => $site_name,
					};
					$cfg = $r;
				}
			}
		} else {
			if ($line =~ /^\s*site\s+@(\w+)\s*{\s*$/) {
				$site_name = $1;
				$in_site = 1;
			}
		}
	}
}

my $ts_relbase = "$ts_dir/tests/$ts_name";
my $ts_base = realpath($ts_relbase) or fatal "realpath $ts_relbase";
-d $ts_base or die "$ts_base not a directory";

my $ts_fn = realpath($0);
$ts_fn =~ /\Q$TSUITE_REL_FN\E$/ or
    die "$ts_fn: does not contain $TSUITE_REL_FN";
my $src_dir = $`;

chdir($src_dir) or die "chdir $src_dir";
my $diff = join '', `gmake scm-diff`;

my $ts_cfg = slurp "$ts_base/cfg";

parse_conf($ts_cfg);

# sanity checks
fatalx "no client(s) specified" unless exists $gcfg{client};

my $clients = ref $gcfg{client} eq "ARRAY" ?
    $gcfg{client} : [ $gcfg{client} ];

foreach my $client_spec (@$clients) {
	my @fields = split /:/, $client_spec;
	my $host = shift @fields;
	my %n = (
		host	=> $host,
		type	=> "client",
	);
	foreach my $field (@fields) {
		my ($k, $v) = split /=/, $field, 2;
		$n{$k} = $v;
	}
	debug_msg "parsed client: $host";
	push @cli, \%n;
}

foreach my $n (@mds) {
	fatalx "no zpool_args specified for $n->{host}"
	    unless exists $n->{zpool_args};
}

foreach my $n (@mds, @ios) {
	if (exists $n->{fmtcmd}) {
		if (ref $n->{fmtcmd} eq "ARRAY") {
			for my $cmd (@$n->{fmtcmd}) {
				$cmd =~ s/^/sudo /;
			}
		} else {
			$n->{fmtcmd} = [ "sudo " . $n->{fmtcmd} ];
		}
	} else {
		$n->{fmtcmd} = [ ];
	}
}

my $step_timeout = 60 * 7;		# single op interval timeout
my $total_timeout = 60 * 60 * 8;	# entire client run duration

my $mount_slash = "sudo mount_slash/mount_slash";
my $slashd = "sudo slashd/slashd";
my $slictl = "slictl/slictl";
my $sliod = "sudo sliod/sliod";
my $slkeymgt = "slkeymgt/slkeymgt";
my $slmctl = "slmctl/slmctl";
my $slmkfs = "sudo slmkfs/slmkfs";
my $slmkjrnl = "sudo slmkjrnl/slmkjrnl";
my $zfs_fuse = "sudo utils/zfs-fuse.sh";
my $zpool = "sudo utils/zpool.sh";

my $repo_url = 'ssh://source/a';

my $ssh_opts = "-oControlPath=yes " .
    "-oControlPersist=yes " .
    "-oControlPath=/tmp/tsuite.ss.%h " .
    "-oKbdInteractiveAuthentication=no " .
    "-oNumberOfPasswordPrompts=1";
my $ssh = "ssh $ssh_opts ";

if (exists $gcfg{bounce_host}) {
	$ssh .= " $gcfg{bounce_host} ssh $ssh_opts ";
}

if ($opts{m}) {
	pipe RD, WR;
} else {
	*WR = *STDERR;
}

eval {

local $SIG{ALRM} = sub { fatal "timeout exceeded" };

# Generate authbuf key
my $authbuf_minkeysize =
    `grep AUTHBUF_MINKEYSIZE $src_dir/slash2/include/authbuf.h`;
my $authbuf_maxkeysize =
    `grep AUTHBUF_MAXKEYSIZE $src_dir/slash2/include/authbuf.h`;
$authbuf_minkeysize =~ s/(?<=AUTHBUF_MINKEYSIZE).*/$&/e;
$authbuf_maxkeysize =~ s/(?<=AUTHBUF_MAXKEYSIZE).*/$&/e;
my $authbuf_keysize = $authbuf_minkeysize +
    int rand($authbuf_maxkeysize - $authbuf_minkeysize + 1);

my $authbuf;
open R, "<", "/dev/urandom" or fatal "open /dev/urandom",
read(R, $authbuf, $authbuf_keysize) == $authbuf_keysize
    or fatal "read /dev/urandom";
close R;

my @pids;

my $n;
# Checkout the source and build it
foreach $n (@mds, @ios, @cli) {
	$n->{TMPDIR} = "/tmp" unless exists $n->{TMPDIR};
	$n->{base_dir} = "$n->{tmpdir}/tsuite/run";
	$n->{src_dir} = "$n->{base_dir}/src";
	$n->{data_dir} = "$n->{base_dir}/data";
	$n->{slcfg} = "$n->{src_dir}/$TSUITE_REL_BASE/tests/$ts_name/cfg";
	$n->{ctlsock} = "$n->{base_dir}/ctlsock";

	my @mkdir = (
		"$n->{ts_dir}/coredumps",
		$n->{src_dir},
		$n->{data_dir}
	);

	if ($n->{type} eq "client") {
		$n->{mp} = "$n->{base_dir}/mp";
		push @mkdir, $n->{mp};
	}

	my $authbuf_fn = "$n->{data_dir}/authbuf.key";

	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		set -e
		@{[ $opts{v} ? "set -x" : "" ]}

		rm -rf $n->{base_dir}
		mkdir -p @mkdir

		cd $n->{src_dir}
		git clone $repo_url .
		./bootstrap.sh

		cat <<'___MKCFG_EOF' > mk/local.mk
$gcfg{mkcfg}
___MKCFG_EOF

		patch -p0 <<'___PATCH_EOF'
$diff
___PATCH_EOF

		make build >/dev/null

		touch $authbuf_fn
		chmod 400 $authbuf_fn
		base64 -d <<'___AUTHBUF_EOF' > $authbuf_fn;
$authbuf
___AUTHBUF_EOF
		truncate -s $authbuf_keysize $authbuf_fn
EOF
}

waitjobs \@pids, $step_timeout;

# Create the MDS file systems
foreach $n (@mds) {
	debug_msg "initializing slashd environment: $n->{res_name} : $n->{host}";

	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		sudo pkill zfs-fuse || true
		sleep 5 &
		$zfs_fuse &
		sleep 2
		@$n->{fmtcmd}
		$zpool destroy $n->{zpool_name} || true
		$zpool create -f $n->{zpool_name} $n->{zpool_args}
		$slmkfs /$n->{zpool_name}
		sudo umount /$n->{zpool_name}
		sudo pkill zfs-fuse

		$slmkjrnl -D $n->{data_dir} -f
EOF
}

waitjobs \@pids, $step_timeout;

# Launch MDS servers
my @mds_pids;
foreach $n (@mds, @ios, @cli) {
	debug_msg "launching slashd: $n->{res_name} : $n->{host}";

	push @mds_pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		mkdir -p $n->{data_dir}

		while :; do
			$slashd -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
			if crashed; then
				collect gdb
			fi
		done
EOF
}

# Create the IOS file systems
foreach $n (@ios) {
	debug_msg "initializing sliod environment: $n->{res_name} : $n->{host}";
	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		mkdir -p $n->{data_dir}
		mkdir -p $n->{fsroot}
		$slmkfs -Wi $n->{fsroot}
EOF
}

waitjobs \@pids, $step_timeout;

# Launch the IOS servers
my @ios_pids;
foreach $n (@ios) {
	debug_msg "launching sliod: $n->{res_name} : $n->{host}";

	push @ios_pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		while :; do
			$sliod -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
			if crashed;
				collect gdb report
			fi
		done
EOF
}

# Launch the client mountpoints
my @cli_pids;
foreach $n (@cli) {
	debug_msg "launching mount_slash: $n->{host}";

	push @cli_pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$mount_slash -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir} $n->{mp}
EOF
}

my $setup = <<EOF;
	export RANDOM=/dev/shm/r000
	test_src_dir=$n->{src_dir}/$TSUITE_REL_BASE/tests/$ts_name/cmd
	cd \$test_src_dir

	runtest()
	{
		export LOCAL_TMP=$n->{TMPDIR}/\$test
		rm -rf \$LOCAL_TMP
		mkdir -p \$LOCAL_TMP
		cd \$LOCAL_TMP

		\$test_src_dir/\$test
	}
EOF

# Run the client application tests
foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		dd if=/dev/urandom of=\$RANDOM bs=1048576 count=1024

		$setup

		for test in *; do
			time0=\$(date +%s.%N)
			(runtest \$test $n)
			time1=\$(date +%s.%N)
		done
EOF
}

waitjobs \@pids, $total_timeout;

foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		$setup

		# run all tests in parallel without faults for
		# performance regressions and exercise
		time0=\$(date +%s.%N)
		for test in *; do
			(runtest \$test $n &)
		done
		wait
		time1=\$(date +%s.%N)
EOF
}

waitjobs \@pids, $total_timeout;

foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		$setup

		# now run the entire thing again injecting faults at
		# random places to test failure tolerance
		for test in *; do
			time0=\$(date +%s.%N)
			(runtest \$test $n)
			time1=\$(date +%s.%N)
		done
EOF
}

do {
	sleep $random
	kill;
} while (waitjobs \@pids, $total_timeout, NONBLOCK);

# Unmount mountpoints
foreach $n (@cli) {
	debug_msg "unmounting mount_slash: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		sudo umount $n->{mp}
EOF
}

waitjobs \@pids, $step_timeout;

# Kill IOS daemons
foreach $n (@ios) {
	debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$slictl -S $n->{ctlsock} stop
EOF
}

waitjobs \@pids, $step_timeout;

# Kill MDS daemons
foreach $n (@mds) {
	debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$slmctl -S $n->{ctlsock} stop
EOF
}

waitjobs \@pids, $step_timeout;

waitjobs \@cli_pids, $step_timeout;
waitjobs \@ios_pids, $step_timeout;
waitjobs \@mds_pids, $step_timeout;

}; # end of eval

my $emsg = $@;

if ($opts{R}) {
}

if ($opts{m}) {
	close WR;

	my @lines = <RD>;

	if (@lines || $emsg) {
		my $smtp = Net::SMTP->new('mailer.psc.edu');

		my $to = 'slash2-devel+report@psc.edu';
		my $from = 'slash2-devel@psc.edu';

		$smtp->mail($from);
		$smtp->to($to);
		$smtp->data();
		$smtp->datasend(<<EOM);
To: $to
From: $from
Subject: tsuite report

Output from run by @{[$ENV{SUDO_USER} || $ENV{USER}]}:

@lines
error: $emsg
EOM
		$smtp->dataend();
		$smtp->quit;
	}
} else {
	warn "error: $emsg\n" if $emsg;
	exit 1 if $emsg;
}
