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

sub init_env {
	my $r = @_;

	while (my ($k, $v) = each %$r) {
		print "export $k='$v'";
	}
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

our ($rootdir, $svnroot, @cli, $src, $intvtimeout, $runtimeout, $logbase);

# Sanity check variables
fatalx "rootdir not defined"	unless defined $rootdir;
fatalx "svnroot not defined"	unless defined $svnroot;
fatalx "cli not defined"	unless defined @cli;
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
my $zpool = "$slbase/utils/zpool.sh";
my $zfs_fuse = "$slbase/utils/zfs-fuse.sh";
my $slmkjrnl = "$slbase/slmkjrnl/slmkjrnl";
my $slimmns_format = "$slbase/slimmns/slimmns_format";
my $odtable = "$src/psc_fsutil_libs/utils/odtable";
my $ion_bmaps_odt = "/var/lib/slashd/ion_bmaps.odt";

my $ssh_init = "set -e; cd $base";

# Setup configuration
my $conf = slash_conf(base => $base);
open SLCONF, ">", "$base/slash.conf" or fatal "open $base/slash.conf";
print SLCONF $conf;
close SLCONF;

my @mds;
my @ion;
my $def_lnet;

sub new_res {
	my ($host, $site) = @_;

	my %r = (
		host => $host,
		site => $site,
	);
	return \%r;
}

sub res_done {
	my ($r) = @_;

	if ($r->{type} eq "mds") {
		push @mds, $r;
	} else {
		push @ion, $r;
	}
}

# Parse configuration for MDS and IONs
sub parse_conf {
	my @conf_lines = split $conf;
	my $in_site = 0;
	my $site_name;
	my $r = undef;
	my @lines = split $conf;

	for (my $ln = 0; $ln < @lines; $ln++) {
		my $line = $lines[$ln];

		if ($in_site) {
			if ($r) {
				if ($line =~ /^\s*type\s*=\s*(\S+)\s*;\s*$/) {
					$r->{type} = $1;
				} elsif ($line =~ /^\s*#\s*\@zfspool\s*=\s*(\w+)\s+(.*)\s*$/) {
					$r->{zpoolname} = $1;
					$r->{zpool_args} = $2;
				} elsif ($line =~ /^\s*fsroot\s*=\s*(\S+)\s*;\s*$/) {
					($r->{fsroot} = $1) =~ s/^"|"$//g;
				} elsif ($line =~ /^\s*ifs\s*=\s*(.*)$/) {
					my $tmp = $1;

					for (; ($line = $lines[$ln]) !~ /;/; $ln++) {
						$tmp .= $line;
					}
					$tmp =~ s/;\s*$//;
					$r->{ifs} = [ split /\s*,\s*/ ];
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
			} elsif (/^\s*global\s+net\s*=\s*(".*?"|.*?);\s*$/) {
				($def_lnet = $1) =~ s/^"|"$//g;
			}
		}
	}

	fatalx "could not parse default LNET network from config:\n$conf" unless $def_lnet;
}

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
	debug_msg "MDS environment: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    $zfs_fuse &
	    sleep 2
	    $zpool create $i->{zpoolname} $i->{zpool_args}
	    $slimmns_format /$i->{zpoolname}
	    sync; sync
	    umount /$i->{zpoolname}
	    kill %1

	    $slmkjrnl
	    $odtable -C -N $ion_bmaps_odt
EOF
}

waitjobs $intvtimeout;

# Launch MDS
foreach $i (@mds) {
	debug_msg "MDS: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    @{[init_env()]}
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
	debug_msg "ION environment: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    $slimmns_format -i $i->{fsroot}
EOF
}

waitjobs $intvtimeout;

# Launch MDS
foreach $i (@ion) {
	debug_msg "ION: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
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
	debug_msg "mount_slash: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
		$ssh_init
		@{[init_env($i->{env})]}
		screen -d -m -S MSL.$tsid \\
		    sh -c "gdb -f -x $slbase/utils/tsuite/msl.gdbcmd $slbase/mount_slash/mount_slash; umount $mp"
EOF
}

waitjobs $intvtimeout;

# Wait for the client control sockets to appear
alarm $intvtimeout;
sleep 1 until scalar @{[ glob "$base/ctl/msl.*.sock" ]} == @cli;
alarm 0;

# Run the client applications
foreach $i (@cli) {
	debug_msg "client: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    screen -d -m -S MSL.$tsid sh -c "sh $base/cli_cmd $i->{host} || \$SHELL"
	    while screen -ls | grep -q CLIENT.$tsid; do
		[ \$SECONDS -lt $runtimeout ]
		sleep 1
	    done
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
	debug_msg "stopping sliod: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    $slbase/slictl/slictl -S $base/ctl/sliod.%h.sock -c exit
EOF
}

waitjobs $intvtimeout;

# Kill MDS's
foreach $i (@mds) {
	debug_msg "stopping slashd: $i->{host}";
	runcmd "ssh $i->{host} sh", <<EOF;
	    $ssh_init
	    $slbase/slmctl/slmctl -S $base/ctl/slashd.%h.sock -c exit
EOF
}

waitjobs $intvtimeout;

foreach $i (@cli) {
	debug_msg "force quitting mount_slash screens: $i->{host}";
	system "ssh $i->{host} screen -S MSL.$tsid -X quit";
}

foreach $i (@ion) {
	debug_msg "force quitting sliod screens: $i->{host}";
	system "ssh $i->{host} screen -S SLIOD.$tsid -X quit";
}

foreach $i (@mds) {
	debug_msg "force quitting slashd screens: $i->{host}";
	system "ssh $i->{host} screen -S SLMDS.$tsid -X quit";
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
