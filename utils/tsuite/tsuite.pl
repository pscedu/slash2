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

my %opts;
my @mds;
my @ios;
my @cli;

# Low level utility routines.
sub fatalx {
	die "$0: @_";
}

sub fatal {
	die "$0: @_: $!";
}

sub usage {
	die <<EOF
usage: $0 [-mqR] [test-name]

options:
  -m	send e-mail report
  -R	record results to database
  -v	verbose (debugging) output

EOF
}

sub init_env {
	my $n = shift;

	$n->{TMPDIR} = "/tmp" unless exists $n->{TMPDIR};
	$n->{src_dir} = "$n->{TMPDIR}/src" unless exists $n->{src_dir};

	return <<EOF;
	set -e
	@{[ $opts{v} ? "set -x" : "" ]}
	mkdir -p $n->{src_dir}
	cd $n->{src_dir}
	@{[map { "export $_='$n->{env}{$_}'\n" } keys $n->{env} ]}
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

sub waitjobs {
	my ($to) = @_;
	local $_;

	alarm $to;
	until (wait == -1) {
		fatalx "child process exited nonzero: " . ($? >> 8) if $?;
	}
	alarm 0;
}

sub runcmd {
	my ($cmd, $in) = @_;

	my $infh;
	open3($infh, ">&WR", ">&WR", $cmd);
	print $infh $in;
	close $infh;
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
sub res_done {
	my ($r) = @_;

	if ($r->{type} eq "mds") {
		fatalx "MDS $r->{res_name} has no \@zfspool configuration"
		    unless $r->{zpool_args};
		push @mds, $r;
	} else {
		fatalx "MDS $r->{res_name} has no \@prefmds configuration"
		    unless $r->{prefmds};
		push @ios, $r;
	}
}

# Parse configuration for MDS and IOS resources
sub parse_conf {
	my $cfg = shift;

	my $in_site = 0;
	my $site_name;
	my @lines = split /\n/, $cfg;
	my $r;

	for (my $ln = 0; $ln < @lines; $ln++) {
		my $line = $lines[$ln];
		if ($in_site) {
			if ($r) {
				if ($line =~ /^\s*type\s*=\s*(\S+)\s*;\s*$/) {
					$r->{type} = $1;
				} elsif ($line =~ /^\s*id\s*=\s*(\d+)\s*;\s*$/) {
					$r->{id} = $1;
				} elsif ($line =~ m{^\s*#\s*\@zfspool\s*=\s*(\w+)\s+(.*)\s*$}) {
					$r->{zpool_name} = $1;
					$r->{zpool_cache} = "$1.zcf";
					$r->{zpool_args} = $2;
				} elsif ($line =~ /^\s*#\s*\@prefmds\s*=\s*(\w+\@\w+)\s*$/) {
					$r->{prefmds} = $1;
				} elsif ($line =~ /^\s*fsroot\s*=\s*(\S+)\s*;\s*$/) {
					($r->{fsroot} = $1) =~ s/^"|"$//g;
				} elsif ($line =~ /^\s*ifs\s*=\s*(.*)$/) {
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

getopts("mRv", \%opts) or usage();
usage() if @ARGV > 1;

my $ts_dir = dirname($0);
my $ts_name = "suite0";
$ts_name = $ARGV[0] if @ARGV > 0;

my $ts_relbase = "$ts_dir/tests/$ts_name";
my $ts_base = realpath($ts_relbase) or fatal "realpath $ts_relbase";
-d $ts_base or die "$ts_base not a directory";

my $ts_fn = realpath($0);
my $TSUITE_REL_FN = 'slash2/utils/tsuite/tsuite.pl';
$ts_fn =~ /\Q$TSUITE_REL_FN\E$/ or
    die "$ts_fn: does not contain $TSUITE_REL_FN";
my $src_dir = $`;

chdir($src_dir) or die "chdir $src_dir";
my $diff = join '', `gmake scm-diff`;

my $ts_cfg = slurp "$ts_base/cfg";
my %cfg;

while (grep /^#\s*\@(\w+)\s+(.*)/cgm, $ts_cfg) {
	my $name = $1;
	my $value = $2;
	debug_msg "parsed parameter: $name=$value";
	push_vectorize(\%cfg, $name, $value);
}

my $op_timeout = 60 * 7;		# single op interval timeout
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

my $ssh = "ssh -oControlPath=yes " .
    "-oControlPersist=yes " .
    "-oKbdInteractiveAuthentication=no " .
    "-oNumberOfPasswordPrompts=1";

if (exists $cfg{bounce}) {
	$ssh .= " $cfg{bounce} ssh";
}

fatalx "no client(s) specified" unless exists $cfg{client};

my $clients = ref $cfg{client} eq "ARRAY" ?
    $cfg{client} : [ $cfg{client} ];

foreach my $client_spec (@$clients) {
	my @fields = split /:/, $client_spec;
	my $host = shift @fields;
	my %n = (
		host	=> $host,
		type	=> "client",
		src_dir	=> ,
	);
	foreach my $field (@fields) {
		my ($k, $v) = split /=/, $field, 2;
		$n{$k} = $v;
	}
	debug_msg "parsed client: $host";
	push @cli, \%n;
}

if ($opts{m}) {
	pipe RD, WR;
} else {
	*WR = *STDERR;
}

eval {

local $SIG{ALRM} = sub { fatal "timeout exceeded" };

my $n;
# Checkout the source and build it
foreach $n (@mds, @ios, @cli) {
	$n->{src_dir} = "$n->{base}/src";

	my $mp = "$base/mp";
	my $datadir = "$base/data";

	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		mkdir -p $n->{ts_dir}/coredumps

		git clone $repo_url $n->{src_dir}
		cd $n->{src_dir}
		./bootstrap.sh

		cat <<'___MKCFG_EOF' > mk/local.mk
$cfg{mkcfg}
___MKCFG_EOF

		patch -p0 <<'___PATCH_EOF'
$diff
___PATCH_EOF

		make build >/dev/null
EOF
}

waitjobs $op_timeout;

parse_conf($ts_cfg);

# Create the MDS file systems
foreach $n (@mds) {
	debug_msg "initializing slashd environment: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		pkill zfs-fuse || true
		sleep 5 &
		$zfs_fuse &
		sleep 2
		$zpool destroy $n->{zpool_name} || true
		$zpool create -f $n->{zpool_name} $n->{zpool_args}
		#$zpool set cachefile=$n->{zpool_cache} $n->{zpool_name}
		$slmkfs /$n->{zpool_name}
		umount /$n->{zpool_name}
		pkill zfs-fuse

		mkdir -p $n->{datadir}

		$slmkjrnl -D $n->{datadir} -f
EOF
}

waitjobs $op_timeout;

# Generate and capture the authbuf key
$n = $mds[0];
my $infh;
my $outfh;
open3($infh, $outfh, ">&WR", "$ssh $n->{host} sh")
    or fatal "$ssh $n->{host} sh";
print $infh <<EOF;
	@{[init_env($n)]}
	$slkeymgt -c -D $n->{datadir}
	base64 $n->{authbuf}
EOF
$n->{has_authbuf} = 1;
close $infh;
my $authbuf = <$outfn>;
close $outfh;

# Launch MDS servers
my $first = 1;
foreach $n (@mds, @ios, @cli) {
	debug_msg "launching slashd: $n->{res_name} : $n->{host}";

	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		mkdir -p $n->{datadir}

		@{[$n->{has_authbuf} ? "" : <<EOC]}
		base64 -dd <<'___AUTHBUF_EOF' > $n->{authbuf}
$authbuf
___AUTHBUF_EOF
EOC

		$slashd -S $n->{ctlsock} -f $n->{slcfg} -D $n->{datadir}
EOF

	$first = 0;
}

waitjobs $op_timeout;

# Wait for the server control sockets to appear
alarm $op_timeout;
sleep 1 until scalar @{[ glob "$base/ctl/slashd.*.sock" ]} == @mds;
alarm 0;

# Create the IOS file systems
foreach $n (@ios) {
	debug_msg "initializing sliod environment: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		mkdir -p $n->{datadir}
		mkdir -p $n->{fsroot}
		$slmkfs -Wi $n->{fsroot}

		base64 -d <<'___AUTHBUF_EOF' > $n->{authbuf}
$authbuf
___AUTHBUF_EOF

EOF
}

waitjobs $op_timeout;

# Launch the IOS servers
foreach $n (@ios) {
	debug_msg "launching sliod: $n->{res_name} : $n->{host}";

	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$sliod -S $n->{ctlsock} -f $n->{slcfg} -D $n->{datadir}
EOF
}

waitjobs $op_timeout;

# Wait for the server control sockets to appear
alarm $op_timeout;
sleep 1 until scalar @{[ glob "$base/ctl/sliod.*.sock" ]} == @ios;
alarm 0;

# Launch the client mountpoints
foreach $n (@cli) {
	debug_msg "launching mount_slash: $n->{host}";

	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$mount_slash -S $n->{ctlsock} -f $n->{slcfg} -D $n->{datadir} $n->{mp}
EOF
}

waitjobs $op_timeout;

# Wait for the client control sockets to appear
alarm $op_timeout;
sleep 1 until scalar @{[ glob "$base/ctl/msl.*.sock" ]} == @cli;
alarm 0;

# Run the client applications
foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}

		export RANDOM=/dev/shm/r000
		dd if=/dev/urandom of=\$RANDOM bs=1048576 count=1024

		cd $n->{src_dir}/slash2/utils/tsuite/tests/$ts_name/cmd

		for test in *; do
			export LOCAL_TMP=$n->{tmp_dir}/\$test
			rm -rf \$LOCAL_TMP
			mkdir -p \$LOCAL_TMP
			./\$test $n->{host}
		done


	doresults => "perl $src/tools/tsuite_results.pl " .
		($opts{R} ? "-R " : "") . ($opts{m} ? "-m " : "") .
		" $testname $logbase");
EOF
}

# run each test serially without faults for performance regressions

# run all tests in parallel without faults for performance regressions
# and exercise

# now run the entire thing again injecting faults at random places to test
# failure tolerance

waitjobs $total_timeout;

# Unmount mountpoints
foreach $n (@cli) {
	debug_msg "unmounting mount_slash: $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		umount $mp
EOF
}

# Kill IOS daemons
foreach $n (@ios) {
	debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$slictl -S $n->{ctlsock} stop
EOF
}

# Kill MDS daemons
foreach $n (@mds) {
	debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} sh", <<EOF;
		@{[init_env($n)]}
		$slmctl -S $n->{ctlsock} stop
EOF
}

waitjobs $op_timeout;

foreach $n (@cli) {
	debug_msg "force quitting mount_slash: $n->{host}";
	execute "$ssh $n->{host} kill $cli_pid";
}

foreach $n (@ios) {
	debug_msg "force quitting sliod: $n->{res_name} : $n->{host}";
	execute "$ssh $n->{host} kill $ios_pid";
}

foreach $n (@mds) {
	debug_msg "force quitting slashd: $n->{res_name} : $n->{host}";
	execute "$ssh $n->{host} kill $mds_pid";
}

# Clean up files
if ($status == 0) {
	debug_msg "deleting base dir";
	execute "rm -rf $base";
}

}; # end of eval

my $emsg = $@;

if ($opts{m}) {
	close WR;

	my @lines = <RD>;

	if (@lines || $emsg) {
		my $smtp = Net::SMTP->new('mailer.psc.edu');
		$smtp->mail('slash2-devel@psc.edu');
		$smtp->to('slash2-devel@psc.edu');
		$smtp->data();
		$smtp->datasend("To: slash2-devel\@psc.edu\n");
		$smtp->datasend("Subject: tsuite errors\n\n");
		$smtp->datasend("Output from run by ",
		    ($ENV{SUDO_USER} || $ENV{USER}), ":\n\n");
		$smtp->datasend(@lines) if @lines;
		$smtp->datasend("error: $emsg") if $emsg;
		$smtp->dataend();
		$smtp->quit;
	}
}

warn "error: $emsg" if $emsg;
exit 1 if $emsg;
