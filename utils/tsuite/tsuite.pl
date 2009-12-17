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

our ($rootdir, $svnroot, @zestions, @clients, $src,
    @test_users, $intvtimeout, $runtimeout, $logbase);

# Sanity check variables
fatalx "rootdir not defined"	unless defined $rootdir;
fatalx "svnroot not defined"	unless defined $svnroot;
fatalx "zestions not defined"	unless defined @zestions;
fatalx "clients not defined"	unless defined @clients;
fatalx "test_users not defined"	unless defined @test_users;
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
	$base = "$rootdir/zsuite.$tsid";
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
	`svn checkout -q $svnroot $src`;
	fatalx "svn failed" if $?;

	debug_msg "make build";
	`cd $src/zest && make build >/dev/null`;
	fatalx "make failed" if $?;
}

my $zbase = "$src/zest";
my $ssh_init = "set -e; PATH=\$PATH:/cluster/packages/lustre_utils/1.6.6/bin; cd $base";

# Setup server configuration
open ZESTCONF, ">", "$base/zest.conf" or fatal "open $base/zest.conf";
print ZESTCONF zest_conf(base => $base);
close ZESTCONF;

# Setup client commands
open CLICMD, ">", "$base/cli_cmd" or fatal "open $base/cli_cmd";
print CLICMD "set -e;";
print CLICMD cli_cmd(
	fspath	=> $mp,
	base	=> $base,
	src	=> $src,
	logbase	=> $logbase,
	gdbtry	=> "$src/tools/gdbtry.pl ",
	doresults => "perl $src/tools/tsuite_results.pl " .
		($opts{N} ? "-N " : "") . ($opts{m} ? "-m " : "") .
		" $testname $logbase");
close CLICMD;

my ($i, $_c);

# Format the zestions
foreach $i (@zestions) {
	debug_msg "zestFormat: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $zbase/zestFormat/zestFormat -c $base/zest.conf -fQ
	    lfs setstripe $base/zobjroot 0 -1 -1
EOF
}

waitjobs $intvtimeout;

# Launch the zestions
foreach $i (@zestions) {
	debug_msg "zestion: $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    @{[zestion_env($i)]}
	    screen -d -m -S ZSERVER.$tsid \\
		gdb -f -x $zbase/utils/tsuite/zestion.gdbcmd $zbase/zestion/zestiond
EOF
}

waitjobs $intvtimeout;

# Wait for the server control sockets to appear,
# which means the fscks have completed.
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/zestiond.*.sock" ]} == @zestions;
alarm 0;

# Launch the mountpoints
my $nmz = 0;
foreach $_c (@clients) {
	next unless ($i = $_c) =~ s/:mz$//;

	$nmz++;	# Count instances to wait for control sockets later.
	debug_msg "mount_zest: $i";
	runcmd "ssh $i sh", <<EOF;
		$ssh_init
		@{[mz_env($i)]}
		screen -d -m -S MZ.$tsid \\
		    sh -c "gdb -f -x $zbase/utils/tsuite/mz.gdbcmd $zbase/mount_zest/mount_zest; umount $mp"
EOF
}

if ($nmz) {
	waitjobs $intvtimeout;

	# Wait for the client control sockets to appear,
	# which means the mountpoints are up.
	alarm $intvtimeout;
	sleep 1 until scalar @{[ glob "$base/ctl/mz.*.sock" ]} == $nmz;
	alarm 0;
}

# Run the client applications
foreach $_c (@clients) {
	($i = $_c) =~ s/:mz$//;

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
foreach $_c (@clients) {
	next unless ($i = $_c) =~ s/:mz$//;

	debug_msg "unmounting MZ on $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    umount $mp
EOF
}

waitjobs $intvtimeout;

# Kill zestions
foreach $i (@zestions) {
	debug_msg "stopping zestion on $i";
	runcmd "ssh $i sh", <<EOF;
	    $ssh_init
	    $zbase/zctl/zctl -S $base/ctl/zestiond.%h.sock -c exit
EOF
}

waitjobs $intvtimeout;

foreach $_c (@clients) {
	next unless ($i = $_c) =~ s/:mz$//;

	debug_msg "stopping client screens on $i";
	`ssh $i "screen -S MZ.$tsid -X quit"`;
}

foreach $i (@zestions) {
	debug_msg "stopping zestion screens on $i";
	`ssh $i screen -S ZSERVER.$tsid -X quit`;
}

# Clean up files
if ($opts{r}) {
	debug_msg "NOT deleting base dir";
} else {
	debug_msg "deleting base dir";
	`rm -rf $base`;
}

};

my $emsg = $@;

if ($opts{m}) {
	close WR;

	my @lines = <RD>;

	if (@lines || $emsg) {
		my $smtp = Net::SMTP->new('mailer.psc.edu');
		$smtp->mail('zest-devel@psc.edu');
		$smtp->to('zest-devel@psc.edu');
		$smtp->data();
		$smtp->datasend("To: zest-devel\@psc.edu\n");
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
