#!/usr/bin/perl -W
# $Id: tsuite.pl 8434 2009-10-16 20:53:38Z yanovich $

use Getopt::Std;
use POSIX qw(:sys_wait_h);
use IPC::Open3;
use Net::SMTP;
use strict;
use warnings;

sub fatalx {
	die "$0: @_\n";
}

sub fatal {
	die "$0: @_: $!\n";
}

sub usage {
	fatalx "usage: $0 [-mNqr] test\n";
}

my %opts;

sub debug_msg {
	print WR @_, "\n" unless $opts{q};
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

our ($rootdir, $svnroot, @mds, @cli, @ion, $src,
    @test_users, $intvtimeout, $runtimeout, $logbase);

# Sanity check variables
fatalx "rootdir not defined"	unless defined $rootdir;
fatalx "svnroot not defined"	unless defined $svnroot;
fatalx "mds not defined"	unless defined @mds;
fatalx "cli not defined"	unless defined @cli;
fatalx "ion not defined"	unless defined @ion;
fatalx "intvtimeout not defined" unless defined $intvtimeout;
fatalx "runtimeout not defined"	unless defined $runtimeout;
fatalx "svnroot not defined"	unless defined $svnroot;
fatalx "logbase not defined"	unless defined $logbase;

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

fatal "$rootdir" unless -d $rootdir;

my $base;
my $tsid;

# Grab a unique base directory
do {
	$tsid = sprintf "%06d", int rand 1_000_000;
	$base = "$rootdir/slsuite.$tsid";
} while -d $base;

my $mp = "$base/mp";

debug_msg "base dir = $base";

mkdir $base		or fatal "mkdir $base";
mkdir "$base/ctl"	or fatal "mkdir $base/ctl";
mkdir $mp		or fatal "mkdir $mp";
mkdir "$base/fs"	or fatal "mkdir $base/fs";

# Checkout the source and build it
chdir $base		or fatal "chdir $base";
unless (defined($src)) {
	$src = "$base/src";
	debug_msg "svn checkout -q $svnroot $src";
	system "svn checkout -q $svnroot $src";
	fatalx "svn failed" if $?;

	debug_msg "make build";
	system "cd $src/fuse && make build >/dev/null";
	fatalx "make failed" if $?;
	system "cd $src/slash_nara && make zbuild >/dev/null";
	fatalx "make failed" if $?;
	system "cd $src/slash_nara && make build >/dev/null";
	fatalx "make failed" if $?;
}

my $slbase = "$src/slash_nara";
my $zfs_fuse = "$slbase/utils/zfs-fuse.sh";
my $zpool = "$slbase/utils/zpool.sh";
my $slmkjrnl = "$slbase/utils/zpool.sh";
my $ion_bmaps_odt = "/var/lib/slashd/ion_bmaps.odt";

my $ssh_init = "set -e; cd $base";

# Setup server configuration
open SLCONF, ">", "$base/slash.conf" or fatal "open $base/slash.conf";
print SLCONF slash_conf(base => $base);
close SLCONF;

# Setup client commands
open CLICMD, ">", "$base/cli_cmd" or fatal "open $base/cli_cmd";
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

my ($i);

# Create the MDS' environments
foreach $i (@mds) {
	debug_msg "MDS environment: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $zfs_fuse &
	    sleep 2
	    $zpool create $zpool_name
	    $slimmns_format /$zpool_name
	    sync; sync
	    umount /$pool
	    kill %1

	    $slmkjrnl
	    $odtable -C -N $ion_bmaps_odt
EOF
}

waitjobs $intvtimeout;

# Launch MDS
foreach $i (@mds) {
	debug_msg "MDS: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    @{[slashd_env($i)]}
	    screen -d -m -S SLMDS.$tsid \\
		gdb -f -x $slbase/utils/tsuite/slashd.gdbcmd $slbase/slashd/slashd
EOF
}

waitjobs $intvtimeout;

# Wait for the server control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/slashd.*.sock" ]} == @mds;
alarm 0;

# Launch the IONs
foreach $i (@ion) {
	debug_msg "ION environment: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $slimmns_format -i $fsroot
EOF
}

waitjobs $intvtimeout;

# Launch MDS
foreach $i (@ion) {
	debug_msg "ION: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    @{[iod_env($i)]}
	    screen -d -m -S SLIOD.$tsid \\
		gdb -f -x $slbase/utils/tsuite/sliod.gdbcmd $slbase/sliod/sliod
EOF
}

waitjobs $intvtimeout;

# Wait for the server control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/sliod.*.sock" ]} == @ion;
alarm 0;

# Launch the client mountpoints
foreach $i (@cli) {
	debug_msg "mount_slash: $i";
	runcmd "ssh $i sh", <<EOF;
		$ssh_init
		@{[mz_env($i)]}
		screen -d -m -S MSL.$tsid \\
		    sh -c "gdb -f -x $slbase/utils/tsuite/msl.gdbcmd $slbase/mount_slash/mount_slash; umount $mp"
EOF
}

waitjobs $intvtimeout;

# Wait for the client control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/msl.*.sock" ]} == $cli;
alarm 0;

# Run the client applications
foreach $i (@cli) {
	debug_msg "client: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    screen -d -m -S ZCLIENT.$tsid sh -c "sh $base/cli_cmd $i || \$SHELL"
	    while screen -ls | grep -q CLIENT.$tsid; do
		[ \$SECONDS -lt $runtimeout ]
		sleep 1
	    done
EOF
}

waitjobs $runtimeout;

# Unmount mountpoints
foreach $i (@cli) {
	debug_msg "unmounting mount_slash: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    umount $mp
EOF
}

waitjobs $intvtimeout;

# Kill IONs
foreach $i (@ion) {
	debug_msg "stopping sliod: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $slbase/slictl/slictl -S $base/ctl/sliod.%h.sock -c exit
EOF
}

waitjobs $intvtimeout;

# Kill MDS's
foreach $i (@mds) {
	debug_msg "stopping slashd: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $slbase/slmctl/slmctl -S $base/ctl/slashd.%h.sock -c exit
EOF
}

waitjobs $intvtimeout;

foreach $i (@cli) {
	debug_msg "force quitting mount_slash screens: $i";
	system "ssh $i screen -S MSL.$tsid -X quit";
}

foreach $i (@ion) {
	debug_msg "force quitting sliod screens: $i";
	system "ssh $i screen -S SLIOD.$tsid -X quit";
}

foreach $i (@mds) {
	debug_msg "force quitting slashd screens: $i";
	system "ssh $i screen -S SLASHD.$tsid -X quit";
}

# Clean up files
if ($opts{r}) {
	debug_msg "NOT deleting base dir";
} else {
	debug_msg "deleting base dir";
	system "rm -rf $base";
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

print "error: $emsg\n" if $emsg;
exit 1 if $emsg;
