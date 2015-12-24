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

use Cwd qw(realpath);
use Data::Dumper;
use File::Basename;
use Getopt::Std;
use IPC::Open3;
use Net::SMTP;
use POSIX qw(:sys_wait_h :errno_h);
use strict;
use warnings;

my $TSUITE_REL_BASE = 'slash2/utils/tsuite';
my $TSUITE_REL_FN = "$TSUITE_REL_BASE/tsuite.pl";

my $TSUITE_RANDOM = "/dev/shm/tsuite.random";

my %gcfg;	# suite's global configuration settings
my %opts;	# runtime options
my @mds;	# suite MDS nodes
my @ios;	# suite IOS nodes
my @cli;	# suite CLI nodes

getopts("mRu:v", \%opts) or usage();
usage() if @ARGV > 1;

my $ssh_user = "";
$ssh_user = " -l $opts{u} " if $opts{u};

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
usage: $0 [-BmRv] [-u user] [test-name]

options:
  -B		whether to use any configured SSH bounce host
  -m		send e-mail report
  -R		record results to database
  -u user	override user for SSH connection establishment
  -v		verbose (debugging) output

EOF
}

sub init_env {
	my ($n, $first) = @_;

	my @cmd;

	push @cmd, "cd $n->{src_dir}" unless $first;

	return <<EOF;
	set -e
	PS4='[\\h:\$LINENO] + '

	die()
	{
		echo \$@ >&2
		exit 1
	}

	hasprog()
	{
		type \$1 >/dev/null 2>&1
	}

	addpath()
	{
		export PATH=\$PATH:\$1
	}

	runbg()
	{
		(\$@ 0<&- &>/dev/null &) &
	}

	nproc()
	{
		np=\$(command nproc)
		echo \$((np / 2))
	}

	export MAKEFLAGS=-j\$(nproc)
	export SCONSFLAGS=-j\$(nproc)

	addpath /sbin
	addpath $n->{src_dir}/slash2/mount_slash
	addpath $n->{src_dir}/slash2/msctl
	addpath $n->{src_dir}/slash2/slashd
	addpath $n->{src_dir}/slash2/slictl
	addpath $n->{src_dir}/slash2/sliod
	addpath $n->{src_dir}/slash2/slkeymgt
	addpath $n->{src_dir}/slash2/slmctl
	addpath $n->{src_dir}/slash2/slmkfs
	addpath $n->{src_dir}/slash2/slmkjrnl
	addpath $n->{src_dir}/slash2/utils
	addpath $n->{src_dir}/wokfs/mount_wokfs
	addpath $n->{src_dir}/wokfs/wokctl

	data_dir=$n->{data_dir}

	@{[ $opts{v} ? "set -x" : "" ]}

	@cmd
EOF
}

sub debug_msg {
	print WR @_, "\n" if $opts{v};
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

	debug_msg "launching: $cmd";

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
	my $site_id;
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
				if ($line =~ /^\s*(\w+)\s*=\s*(\S+)\s*;\s*$/) {
					$r->{$1} = $2;

					if ($1 eq "nids") {
						my $tmp = $1;

						for (; ($line = $lines[$ln]) !~ /;/; $ln++) {
							$tmp .= $line;
						}
						$tmp =~ s/;\s*$//;
						$r->{host} = (split /\s*,\s*/, $tmp, 1)[0]
						unless exists $r->{host};
					}
				} elsif ($line =~ /^\s*}\s*$/) {

					if ($r->{type} eq "mds") {
						push @mds, $r;
					} elsif ($r->{type} eq "standalone_fs") {
						push @ios, $r
					}

					$r = undef;
				}
			} else {
				if ($line =~ /^\s*resource\s+(\w+)\s*{\s*$/) {
					$r = {
						res_name => $1,
						site => $site_name,
						site_id => $site_id,
					};
					$cfg = $r;
				} elsif ($line =~ /^\s*site_id\s*=\s*(\S+)\s*;\s*$/) {
					$site_id = $1;
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
my $diff = join '', `make scm-diff`;

my $ts_cfg = slurp "$ts_base/cfg";

if ($opts{m}) {
	pipe RD, WR;
} else {
	*WR = *STDERR;
}

parse_conf($ts_cfg);

# sanity checks
fatalx "no client(s) specified" unless exists $gcfg{client};

my $clients = ref $gcfg{client} eq "ARRAY" ?
    $gcfg{client} : [ $gcfg{client} ];

if (exists $gcfg{mkcfg}) {
	if (ref $gcfg{mkcfg} eq "ARRAY") {
	} else {
		$gcfg{mkcfg} = [ $gcfg{mkcfg} ];
	}
} else {
	$gcfg{mkcfg} = [];
}

my $nclients = 0;
foreach my $client_spec (@$clients) {
	my @fields = split /:/, $client_spec;
	my $host = shift @fields;
	my %n = (
		host	=> $host,
		type	=> "client",
		id	=> $nclients++,
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

my $step_timeout = 60 * 7;		# single op interval timeout
my $total_timeout = 60 * 60 * 8;	# entire client run duration

my $zfs_fuse = "zfs-fuse.sh";
my $zpool = "zpool.sh";
my $zfs = "zfs.sh";
my $sudo = "command sudo";

my $repo_url = 'ssh://source/a';

my $ssh_opts = "-oControlMaster=yes " .
    "-oControlPath=/tmp/tsuite.ss.%h.%u " .
    "-oKbdInteractiveAuthentication=no " .
    "-oStrictHostKeyChecking=yes " .
    "-oNumberOfPasswordPrompts=0 $ssh_user";
my $ssh = "ssh $ssh_opts ";

if ($opts{B} && exists $gcfg{bounce_host}) {
	my $ssh_opts_esc = $ssh_opts;
	$ssh_opts_esc =~ s/%/%%/g;
	$ssh .= " -o 'ProxyCommand ssh $ssh_opts_esc $gcfg{bounce_host} nc %h %p' ";
}

foreach my $n (@mds, @ios) {
	if (exists $n->{fmtcmd}) {
		if (ref $n->{fmtcmd} eq "ARRAY") {
			for my $cmd (@{ $n->{fmtcmd} }) {
				$cmd =~ s/^/\n$sudo /;
			}
		} else {
			$n->{fmtcmd} = [ "$sudo " . $n->{fmtcmd} ];
		}
	} else {
		$n->{fmtcmd} = [ ];
	}
}

system("rm -f /tmp/tsuite.ss.*");

eval {

local $SIG{ALRM} = sub { fatal "timeout exceeded" };

# Generate authbuf key
my $authbuf_minkeysize =
    `grep AUTHBUF_MINKEYSIZE $src_dir/slash2/include/authbuf.h`;
my $authbuf_maxkeysize =
    `grep AUTHBUF_MAXKEYSIZE $src_dir/slash2/include/authbuf.h`;
$authbuf_minkeysize =~ s/^.*AUTHBUF_MINKEYSIZE(.*)/eval $1/e;
$authbuf_maxkeysize =~ s/^.*AUTHBUF_MAXKEYSIZE(.*)/eval $1/e;
my $authbuf_keysize = $authbuf_minkeysize +
    int rand($authbuf_maxkeysize - $authbuf_minkeysize + 1);

my $authbuf;
open R, "<", "/dev/urandom" or fatal "open /dev/urandom";
read(R, $authbuf, $authbuf_keysize) == $authbuf_keysize
    or fatal "read /dev/urandom";
close R;
$authbuf =~ s/\0/0/g;

my $fsuuid = sprintf "%#x", rand 1e19;

my @pids;

my %hosts;

my $n;
# Checkout the source and build it
foreach $n (@mds, @ios, @cli) {
	if (ref $n->{host} eq "ARRAY") {
		$n->{host} = pop @{ $n->{host} };
	}

	$n->{TMPDIR} = "/tmp" unless exists $n->{TMPDIR};
	$n->{base_dir} = "$n->{TMPDIR}/tsuite";
	$n->{src_dir} = "$n->{base_dir}/src";
	$n->{data_dir} = "$n->{base_dir}/data";
	$n->{slcfg} = "$n->{src_dir}/$TSUITE_REL_BASE/tests/$ts_name/cfg";
	$n->{ctlsock} = "$n->{base_dir}/$n->{type}.ctlsock";

	my @mkdir = (
		"$n->{base_dir}/coredumps",
		$n->{src_dir},
		$n->{data_dir},
	);

	my @cmds;

	push @cmds, q[setup_src] unless exists $hosts{$n->{host}};
	$hosts{$n->{host}} = 1;

	if ($n->{type} eq "client") {
		$n->{wok_ctlsock} = "$n->{base_dir}/wok.ctlsock";

		$n->{mp} = "$n->{base_dir}/mp";
		push @mkdir, $n->{mp};

		push @cmds, qq[dd if=/dev/urandom of=$TSUITE_RANDOM bs=1048576 count=1024];
	}

	my $authbuf_fn = "$n->{data_dir}/authbuf.key";

	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n, 1)]}

		setup_src()
		{
			$sudo umount -l -f $n->{data_dir} || true
			$sudo rm -rf $n->{base_dir}
			mkdir -p @mkdir

			cd $n->{src_dir}
			git clone $repo_url .
			./bootstrap.sh

			cat <<'___MKCFG_EOF' > mk/local.mk
@{$gcfg{mkcfg}}
___MKCFG_EOF

			patch -p0 <<'___PATCH_EOF'
$diff
___PATCH_EOF

			make build >/dev/null

			touch $authbuf_fn
			chmod 600 $authbuf_fn
			cat <<'___AUTHBUF_EOF' > $authbuf_fn
$authbuf
___AUTHBUF_EOF
			chmod 400 $authbuf_fn
		}

		@cmds
		mkdir -p @mkdir
EOF
}

waitjobs \@pids, $step_timeout;

# Create the MDS file systems
foreach $n (@mds) {
	debug_msg "initializing slashd environment: $n->{res_name} : $n->{host}";

	my @cmds;
	if (exists $n->{journal}) {
		push @cmds, "$sudo slmkjrnl -f -b $n->{journal} -u $fsuuid";
	} else {
		push @cmds, "$sudo slmkjrnl -f -D $n->{data_dir} -u $fsuuid";
	}

	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}

		$sudo pkill zfs-fuse || true
		sleep 5
		$sudo modprobe fuse
		runbg $sudo $zfs_fuse
		sleep 2

		@{$n->{fmtcmd}}
		$sudo $zpool destroy $n->{zpool_name} || true
		$sudo $zpool create -f $n->{zpool_name} $n->{zpool_args}
		$sudo $zfs set atime=off $n->{zpool_name}
		$sudo slmkfs -I $n->{site_id}:$n->{id} -u $fsuuid /$n->{zpool_name}
		sync
		sleep 2
		$sudo umount /$n->{zpool_name}
		sleep 2
		$sudo pkill zfs-fuse

		@cmds
EOF
}

waitjobs \@pids, $step_timeout;

sub daemon_setup {
	my $n = shift;

	return <<EOF;
	run_daemon()
	{
		export PSC_LOG_FILE=$n->{base_dir}/log
		prog=\$1
		while :; do
			$sudo \$@
			status=\$?
			[ \$status -eq 0 ] && break
			corefile=\$prog.core
			mv -f *core* \$corefile
			if [ -e "\$corefile" ]; then
				[ \$status -gt 128 ] && echo exited via signal \$((status - 128))
				tail \$PSC_LOG_FILE
				gdb -batch -c \$corefile -x \$cmdfile \$prog 2>&1 | $n->{src_dir}/tools/filter-pstack
			fi
		done
	}
EOF
}

# Launch MDS servers
my @mds_pids;
foreach $n (@mds, @ios, @cli) {
	debug_msg "launching slashd: $n->{res_name} : $n->{host}";

	push @mds_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon slashd -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
EOF
}

# Create the IOS file systems
foreach $n (@ios) {
	debug_msg "initializing sliod environment: $n->{res_name} : $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{$n->{fmtcmd}}
		$sudo slmkfs -i -u $fsuuid -I $n->{site_id}:$n->{id} $n->{fsroot}
EOF
}

waitjobs \@pids, $step_timeout;

# Launch the IOS servers
my @ios_pids;
foreach $n (@ios) {
	debug_msg "launching sliod: $n->{res_name} : $n->{host}";

	push @ios_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon sliod -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
EOF
}

# Launch the client mountpoints
my @cli_pids;
foreach $n (@cli) {
	debug_msg "launching mount_slash: $n->{host}";

	push @cli_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon mount_slash -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir} $n->{mp}
EOF
}

sub test_setup {
	my $n = shift;

	return <<EOF;
	export RANDOM=$TSUITE_RANDOM
	test_src_dir=$n->{src_dir}/$TSUITE_REL_BASE/tests/$ts_name/cmd
	cd \$test_src_dir

	msctl()
	{
		$n->{src_dir}/slash2/msctl/msctl -S $n->{ctlsock} \$@
	}
	export -f msctl

	run_test()
	{
		test=\$1
		id=\$2
		max=\$3

		export LOCAL_TMP=$n->{TMPDIR}/\$test
		export SRC=$n->{src_dir}
		rm -rf \$LOCAL_TMP
		mkdir -p \$LOCAL_TMP
		cd \$LOCAL_TMP

		\$test_src_dir/\$test \$id \$max
	}

	run_timed_test()
	{
		test=\$1
		id=\$2

		time0=\$(date +%s.%N)
		run_test \$@
		time1=\$(date +%s.%N)
		delta=\$(echo \$time1 - \$time0 | bc)
		echo %TSUITE_RESULT% \$test:\$id \$delta
	}

	dep()
	{
		for prog; do
			case \$prog in
			iozone)	addpath $n->{src_dir}/distrib/iozone ;;
			sft)	addpath $n->{src_dir}/sft ;;
			esac

			hasprog \$prog && continue

			case \$prog in
			iozone)	cd $n->{src_dir}/distrib/iozone
				make linux-AMD64
				;;
			*)	die "unhandled dependency \$i"
				;;
			esac
		done
	}
	export -f dep
EOF
}

# Set 1: run the client application tests, serially, measuring stats on
# each so we can present historical performance analysis.
foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		for test in *; do
			run_timed_test \$test $n->{id} $nclients
		done
EOF
}

waitjobs \@pids, $total_timeout;

# Set 2: run all tests in parallel without faults for performance
# regressions and exercise.
foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		run_all_tests()
		{
			for test in *; do
				(run_test \$test $n->{id} $nclients &)
			done
			wait
		}

		run_timed_test run_all_tests $n->{id} $nclients
EOF
}

waitjobs \@pids, $total_timeout;

# Set 3: now run the entire suite again, injecting faults at random
# places to test failure tolerance.
foreach $n (@cli) {
	debug_msg "client: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		for test in *; do
			run_test \$test $n->{id} $nclients
		done
EOF
}

sub add_fault {
	my ($ra, $n, $name) = @_;

	my $ctlcmd = "slictl";
	if ($n->{type} eq "client") {
		$ctlcmd = "msctl";
	} elsif ($n->{type} eq "mds") {
		$ctlcmd = "slmctl";
	}

	push @$ra, {
		cmd => "$sudo $ctlcmd -p faults.$name.count+=1",
		host => $n->{host},
	};
}

my @misfortune;

#foreach $n (@cli) {
#	push @misfortune, {
#		cmd => "$sudo wokctl -S $n->{wok_ctlsock} reload 0",
#		host => $n->{host},
#	};
#	add_fault(\@misfortune, $n, 'msl.request_timeout');
#	add_fault(\@misfortune, $n, 'msl.read_cb');
#	add_fault(\@misfortune, $n, 'msl.readrpc_offline');
#}

foreach $n (@mds) {
	push @misfortune, {
		cmd => "$sudo kill -HUP \$(slmctl -S $n->{ctlsock} -p pid)",
		host => $n->{host},
	};
}

foreach $n (@ios) {
	push @misfortune, {
		cmd => "$sudo kill -HUP \$(slictl -S $n->{ctlsock} -p pid)",
		host => $n->{host},
	};
	#add_fault(\@misfortune, $n, 'sliod.fsio_read_fail');
	#add_fault(\@misfortune, $n, 'sliod.crcup_fail');
}

sub rand_array {
	my ($ra) = @_;

	return $ra->[int rand(@$ra)];
}

do {
	my $random = 5 * 60 + int rand(10 * 60);
	sleep $random;

	my $a = rand_array(\@misfortune);
	my @killpid;
	push @killpid, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		$a->{cmd}
EOF
	waitjobs \@killpid, $step_timeout;

} while (waitjobs \@pids, $total_timeout, NONBLOCK);

# Unmount mountpoints
foreach $n (@cli) {
	debug_msg "unmounting mount_slash: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		$sudo umount $n->{mp}
EOF
}

waitjobs \@pids, $step_timeout;

# Kill IOS daemons
foreach $n (@ios) {
	debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		$sudo slictl -S $n->{ctlsock} stop
EOF
}

waitjobs \@pids, $step_timeout;

# Kill MDS daemons
foreach $n (@mds) {
	debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
	runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		$sudo slmctl -S $n->{ctlsock} stop
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
