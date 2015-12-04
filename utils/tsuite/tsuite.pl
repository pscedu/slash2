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

use Getopt::Std;
use POSIX qw(:sys_wait_h :errno_h);
use IPC::Open3;
use Net::SMTP;
use strict;
use warnings;

my %opts;

sub fatalx {
	die "$0: @_";
}

sub fatal {
	die "$0: @_: $!";
}

sub usage {
	fatalx "usage: $0 [-mNqr] test-file";
}

sub init_env {
	my %r = @_;
	return join '', map { "export $_='$r{$_}'\n" } keys %r;
}

sub debug_msg {
	print WR @_, "\n" unless $opts{q};
}

sub execute {
	debug_msg "executing: ", join ' ', @_;
	system join ' ', @_;
}

sub slurp {
	my ($fn) = @_;
	local $/;

	open G, "<", "$fn" or fatal $fn;
	my $dat = <G>;
	close G;
	return ($dat);
}

sub dump_res {
	my ($res) = @_;
	print "resource:\n";
	while (my ($k, $v) = each %$res) {
		print "  $k: $v\n";
	}
	print "\n";
}

getopts("mNqr", \%opts) or usage();
usage() unless @ARGV == 1;

if ($opts{m}) {
	pipe RD, WR;
} else {
	*WR = *STDERR;
}

eval {

require $ARGV[0];
my $testname = $ARGV[0];
$testname =~ s!.*/!!;

our ($rootdir, $giturl, @cli, $src, $intvtimeout, $runtimeout,
    $logbase, $global_env);

# Sanity check variables
fatalx "rootdir not defined"	unless defined $rootdir;
fatalx "giturl not defined"	unless defined $giturl;
fatalx "cli not defined"	unless defined @cli;
fatalx "intvtimeout not defined" unless defined $intvtimeout;
fatalx "runtimeout not defined"	unless defined $runtimeout;
fatalx "logbase not defined"	unless defined $logbase;
fatalx "global_env not defined"	unless defined $global_env;

local $SIG{ALRM} = sub { fatal "interval timeout exceeded" };

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

fatal $rootdir unless -d $rootdir;

my $base;
my $tsid;

# Grab a unique base directory
do {
	$tsid = sprintf "%06d", int rand 1_000_000;
	$base = "$rootdir/slsuite.$tsid";
} while -d $base;

debug_msg "base dir: $base";

my $mp = "$base/mp";
my $datadir = "$base/data";

mkdirs $base		or fatal "mkdirs $base";
mkdir $datadir		or fatal "mkdir $datadir";
mkdir "$base/ctl"	or fatal "mkdir $base/ctl";
mkdir "$base/fs"	or fatal "mkdir $base/fs";
mkdir $mp		or fatal "mkdir $mp";

# Checkout the source and build it
chdir $base		or fatal "chdir $base";
my $build = 0;
unless (defined($src)) {
	$src = "$base/src";
	execute "git clone $giturl $src";
	fatalx "git failed" if $?;

	$build = 1;
}

my $slbase = "$src/slash2";
my $tsbase = "$slbase/utils/tsuite";
my $clicmd = "$base/cli_cmd.sh";

my $zpool = "$slbase/utils/zpool.sh";
my $zfs_fuse = "$slbase/utils/zfs-fuse.sh";
my $slmkjrnl = "$slbase/slmkjrnl/slmkjrnl";
my $slmctl = "$slbase/slmctl/slmctl";
my $slictl = "$slbase/slictl/slictl";
my $slkeymgt = "$slbase/slkeymgt/slkeymgt";
my $slmkfs = "$slbase/slmkfs/slmkfs";

my $ssh_init = <<EOF;
	set -ex

	runscreen()
	{
		scrname=TS.\$1
		shift
		screen -S \$scrname -X quit || true
		screen -d -m -S \$scrname.$tsid "\$\@"
	}

	waitforscreen()
	{
		scrname=\$1
		while screen -ls | grep -q \$scrname.$tsid; do
			[ \$SECONDS -lt $runtimeout ]
			sleep 1
		done
	}

	cd $base;
EOF

# Setup configuration
my $conf = slcfg(base => $base);
open SLCONF, ">", "$base/slcfg" or fatal "open $base/slcfg";
print SLCONF $conf;
close SLCONF;

my @mds;
my @ion;

sub new_res {
	my ($rname, $site) = @_;

	my %r = (
		rname	=> $rname,
		site	=> $site,
	);
	return \%r;
}

sub res_done {
	my ($r) = @_;

	if ($r->{type} eq "mds") {
		fatalx "MDS $r->{rname} has no \@zfspool configuration"
		    unless $r->{zpool_args};
		push @mds, $r;
	} else {
		fatalx "MDS $r->{rname} has no \@prefmds configuration"
		    unless $r->{prefmds};
		push @ion, $r;
	}
}

# Parse configuration for MDS and IONs
sub parse_conf {
	my $in_site = 0;
	my $site_name;
	my $r = undef;
	my @lines = split /\n/, $conf;

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
					$r->{zpool_cache} = "$base/$1.zcf";
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
					$r = new_res($1, $site_name);
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

# Setup client commands
open CLICMD, ">", $clicmd or fatal "open $clicmd";
print CLICMD "set -e;\n";
print CLICMD cli_cmd(
	fspath	=> $mp,
	base	=> $base,
	src	=> $src,
	logbase	=> $logbase,
	gdbtry	=> "$src/tools/gdbtry.pl",
	doresults => "perl $src/tools/tsuite_results.pl " .
		($opts{N} ? "-N " : "") . ($opts{m} ? "-m " : "") .
		" $testname $logbase");
close CLICMD;

parse_conf();

if ($build) {
	execute "cd $src && make build >/dev/null";
	fatalx "make failed" if $?;
}

my ($i);
my $make_authbuf_key = 1;

# Create the MDS file systems
foreach $i (@mds) {
	debug_msg "initializing slashd environment: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env(%$global_env)]}

		screen -S TS.SLMDS -X quit || true

		# run pickle probe tests
		(cd $src && make printvar-CC >/dev/null)

		pkill zfs-fuse || true
		$zfs_fuse &
		sleep 2
		$zpool destroy $i->{zpool_name} || true
		$zpool create -f $i->{zpool_name} $i->{zpool_args}
		$zpool set cachefile=$i->{zpool_cache} $i->{zpool_name}
		$slmkfs /$i->{zpool_name}
		sync; sync
		umount /$i->{zpool_name}
		pkill zfs-fuse

		mkdir -p $datadir

		$slmkjrnl -D $datadir -f

		@{[$make_authbuf_key ? <<EOS : "" ]}
		$slkeymgt -c -D $datadir
		cp -p $datadir/authbuf.key $base
EOS
EOF
	$make_authbuf_key = 0;
}

waitjobs $intvtimeout;

# Launch MDS servers
my $slmgdb = slurp "$tsbase/slashd.gdbcmd";
foreach $i (@mds) {
	debug_msg "launching slashd: $i->{rname} : $i->{host}";

	my %tr_vars = (
		datadir		=> $datadir,
		zpool_cache	=> $i->{zpool_cache},
		zpool_name	=> $i->{zpool_name},
	);

	my $dat = $slmgdb;
	$dat =~ s/%(\w+)%/$tr_vars{$1}/g;

	my $gdbfn = "$base/slashd.$i->{id}.gdbcmd";
	open G, ">", $gdbfn or fatal "write $gdbfn";
	print G $dat;
	close G;

	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env(%$global_env)]}
		cp -p $base/authbuf.key $datadir
		runscreen SLMDS \\
		    gdb -f -x $gdbfn $slbase/slashd/slashd
EOF
}

waitjobs $intvtimeout;

# Wait for the server control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/slashd.*.sock" ]} == @mds;
alarm 0;

# Create the ION file systems
foreach $i (@ion) {
	debug_msg "initializing sliod environment: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env(%$global_env)]}
		mkdir -p $datadir
		mkdir -p $i->{fsroot}
		$slmkfs -Wi $i->{fsroot}
		cp -p $base/authbuf.key $datadir
EOF
}

waitjobs $intvtimeout;

# Launch the ION servers
my $sligdb = slurp "$tsbase/sliod.gdbcmd";
foreach $i (@ion) {
	debug_msg "launching sliod: $i->{rname} : $i->{host}";

	my %tr_vars = (
		datadir		=> $datadir,
		prefmds		=> $i->{prefmds},
	);

	my $dat = $sligdb;
	$dat =~ s/%(\w+)%/$tr_vars{$1}/g;

	my $gdbfn = "$base/sliod.$i->{id}.gdbcmd";
	open G, ">", $gdbfn or fatal "write $gdbfn";
	print G $dat;
	close G;

	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env(%$global_env)]}
		runscreen SLIOD \\
		    gdb -f -x $gdbfn $slbase/sliod/sliod
EOF
}

waitjobs $intvtimeout;

# Wait for the server control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/sliod.*.sock" ]} == @ion;
alarm 0;

# Launch the client mountpoints
my $slcgdb = slurp "$tsbase/msl.gdbcmd";
my $mslid = 0;
foreach $i (@cli) {
	debug_msg "launching mount_slash: $i->{host}";

	my %tr_vars = (
		datadir		=> $datadir,
	);

	my $dat = $slcgdb;
	$dat =~ s/%(\w+)%/$tr_vars{$1}/g;

	my $gdbfn = "$base/msl.$mslid.gdbcmd";
	open G, ">", $gdbfn or fatal "write $gdbfn";
	print G $dat;
	close G;

	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env(%$global_env, %{$i->{env}})]}
		runscreen MSL sh -c "gdb -f -x $gdbfn \\
		    $slbase/mount_slash/mount_slash; umount $mp"
EOF
	$mslid++;
}

waitjobs $intvtimeout;

# Wait for the client control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/msl.*.sock" ]} == @cli;
alarm 0;

# Spawn monitors/gatherers of control stats
foreach $i (@mds) {
	debug_msg "spawning slashd stats tracker: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		runscreen SLMCTL sh -c "sh $tsbase/ctlmon.sh $i->{host} \\
		    $slmctl -S ctl/slashd.$i->{host}.sock -sP -sL -sI || \$SHELL"
EOF
}

waitjobs $intvtimeout;

foreach $i (@ion) {
	debug_msg "spawning sliod stats tracker: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		runscreen SLICTL sh -c "sh $tsbase/ctlmon.sh $i->{host} \\
		    $slictl -S ctl/sliod.$i->{host}.sock -sP -sL -sI || \$SHELL"
EOF
}

waitjobs $intvtimeout;

foreach $i (@cli) {
	debug_msg "spawning mount_slash stats tracker: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		runscreen MSCTL sh -c "sh $tsbase/ctlmon.sh $i->{host} \\
		    $slbase/msctl/msctl -S ctl/msl.$i->{host}.sock -sP -sL -sI || \$SHELL"
EOF
}

waitjobs $intvtimeout;

# Run the client applications
foreach $i (@cli) {
	debug_msg "client: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		runscreen SLCLI sh -c "sh $clicmd $i->{host} || \$SHELL"
		waitforscreen SLCLI
EOF
}

waitjobs $runtimeout;

# Unmount mountpoints
foreach $i (@cli) {
	debug_msg "unmounting mount_slash: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		umount $mp
EOF
}

waitjobs $intvtimeout;

# Kill IONs
foreach $i (@ion) {
	debug_msg "stopping sliod: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		$slictl -S $base/ctl/sliod.%h.sock stop
EOF
}

waitjobs $intvtimeout;

# Kill MDS's
foreach $i (@mds) {
	debug_msg "stopping slashd: $i->{rname} : $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		$slmctl -S $base/ctl/slashd.%h.sock stop
EOF
}

waitjobs $intvtimeout;

foreach $i (@cli) {
	debug_msg "force quitting mount_slash screens: $i->{host}";
	execute "ssh $i->{host} screen -S TS.MSL.$tsid -X quit";
	execute "ssh $i->{host} screen -S TS.MSCTL.$tsid -X quit";
}

foreach $i (@ion) {
	debug_msg "force quitting sliod screens: $i->{rname} : $i->{host}";
	execute "ssh $i->{host} screen -S TS.SLIOD.$tsid -X quit";
	execute "ssh $i->{host} screen -S TS.SLICTL.$tsid -X quit";
}

foreach $i (@mds) {
	debug_msg "force quitting slashd screens: $i->{rname} : $i->{host}";
	execute "ssh $i->{host} screen -S TS.SLMDS.$tsid -X quit";
	execute "ssh $i->{host} screen -S TS.SLMCTL.$tsid -X quit";
}

# Clean up files
if ($opts{r}) {
	debug_msg "deleting base dir";
	execute "rm -rf $base";
}

}; # end of eval

# @set logbase
# @set
logbase = "/home/slog";
rootdir = "/home/sltest";
giturl = 'ssh://source/a';
intvtimeout = 60*7;	# single op interval timeout
runtimeout = 60*60*8;	# entire client run duration
src = "/home/yanovich/code/advsys/p";

# run each test serially without faults for performance regressions

# run all tests in parallel without faults for performance regressions
# and exercise

# now run the entire thing again injecting faults at random places to test
# failure tolerance

my $emsg = $@;


LOCAL_TMP=$TMPDIR/$test_name
rm -rf $LOCAL_TMP
mkdir -p $LOCAL_TMP
dd if=/dev/urandom of=/dev/shm/r000 bs=1048576 count=1024
RANDOM=/dev/shm/r000

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
