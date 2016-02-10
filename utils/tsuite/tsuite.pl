#!/usr/bin/perl -W
# $Id$
# %GPL_START_LICENSE%
# ---------------------------------------------------------------------
# Copyright 2015-2016, Google, Inc.
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
use DBI;
use Data::Dumper;
use File::Basename;
use Getopt::Std;
use IPC::Open3;
use Net::SMTP;
use POSIX qw(:sys_wait_h :errno_h :signal_h);
use strict;
use threads;
use threads::shared;
use warnings;

my $TSUITE_REL_BASE = 'slash2/utils/tsuite';
my $TSUITE_REL_FN = "$TSUITE_REL_BASE/tsuite.pl";

my $TSUITE_RANDOM = "/dev/shm/tsuite.random";

my %gcfg;	# suite's global configuration settings
my %opts;	# runtime options
my @mds;	# suite MDS nodes
my @ios;	# suite IOS nodes
my @cli;	# suite CLI nodes

sub usage {
	die <<EOF
usage: $0 [-BmRv] [-u user] [test-name]

options:
  -B		whether to use any configured SSH bounce host
  -D descr	optional description to include in report
  -m		send e-mail report
  -R		record results to database
  -u user	override user for SSH connection establishment
  -v		verbose (debugging) output

EOF
}

getopts("BmRu:v", \%opts) or usage();
usage() if @ARGV > 1;

my $ssh_user = "";
$ssh_user = " -l $opts{u} " if $opts{u};

my $ts_dir = dirname($0);
my $ts_name = "suite0";
my $ts_user = `id -un`;
$ts_name = $ARGV[0] if @ARGV > 0;

sub min {
	$_[0] < $_[1] ? $_[0] : $_[1];
}

sub stackdump {
	my $s = "";
	my $n = 0;
	while (my @f = caller $n++) {
		$s .= "  $f[3]:$f[1]:$f[2]\n";
	}
	return $s;
}

# Low level utility routines.
sub fatalx {
	die "$0: @_\nstackdump:\n", stackdump();
}

sub fatal {
	fatalx "@_: $!";
}

sub init_env {
	my ($n, $first) = @_;

	my @cmd;

	push @cmd, "cd $n->{src_dir}\n" unless $first;

	if ($n->{type} eq "client") {
		$n->{ctlcmd} = "msctl";
		push @cmd, <<EOF;
			msctl()
			{
				sudo $n->{src_dir}/slash2/msctl/msctl -S $n->{ctlsock} \$@
			}
			export -f msctl

			wokctl()
			{
				sudo $n->{src_dir}/wokfs/wokctl/wokctl -S $n->{wok_ctlsock} \$@
			}
			export -f wokctl
EOF
	} elsif ($n->{type} eq "mds") {
		$n->{ctlcmd} = "slmctl";
		push @cmd, <<EOF;
			slmctl()
			{
				sudo $n->{src_dir}/slash2/slmctl/slmctl -S $n->{ctlsock} \$@
			}
			export -f slmctl
EOF
	} else {
		$n->{ctlcmd} = "slictl";
		push @cmd, <<EOF;
			slictl()
			{
				sudo $n->{src_dir}/slash2/slictl/slictl -S $n->{ctlsock} \$@
			}
			export -f slictl
EOF
	}

	return <<EOF;
	set -e
	set -u
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
	export -f hasprog

	addpath()
	{
		export PATH=\$PATH:\$1
	}
	export -f addpath

	runbg()
	{
		(\$@ 0<&- &>/dev/null &) &
	}

	nproc()
	{
		local np=\$(command nproc)
		echo \$((np / 2))
	}

	daemon_pid()
	{
		$n->{ctlcmd} -Hp pid | awk '{print \$2}'
	}

	decompress_xz()
	{
		if hasprog pixz; then
			pixz -d -t < \$1
		else
			xz -dc \$1
		fi
	}
	export -f decompress_xz

	export MAKEFLAGS=-j\$(nproc)
	export SCONSFLAGS=-j\$(nproc)

	addpath /sbin
	addpath $n->{src_dir}/slash2/slashd
	addpath $n->{src_dir}/slash2/sliod
	addpath $n->{src_dir}/slash2/slkeymgt
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
	print STDERR "[debug] ", @_, "\n" if $opts{v};
}

sub slurp {
	my ($fn) = @_;
	local $/;

	open F, "<", "$fn" or fatal "open $fn";
	my $data = <F>;
	close F;
	return ($data);
}

use constant WF_NONBLOCK => (1 << 0);
use constant WF_NONFATAL => (1 << 1);

sub vsleep {
	select undef, undef, undef, $_[0];
}

use constant MAX_TRIES => 16;

sub waitjobs {
	my ($pids, $to, $flags) = @_;

	$flags ||= 0;

	alarm $to;
	debug_msg "waiting on @$pids";
	for my $pid (@$pids) {
		my $ntries = 0;
 RESTART:
		my $status = waitpid $pid, 0;
		if ($status == -1 && $! == EINTR) {
			fatalx "max tries reached"
			    if $ntries++ > MAX_TRIES;
			vsleep(.001);
			goto RESTART;
		}
		fatal "waitpid $pid" if $status == -1;
		if ($?) {
			my $msg = "child process $pid exited nonzero: " .
			    ($? >> 8);
			if ($flags & WF_NONFATAL) {
				warn "$msg\n";
			} else {
				fatalx $msg;
			}
		}
		debug_msg "reaped child $pid";
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

	debug_msg "launched '$cmd' as pid $pid";

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
			push_vectorize($cfg, $1, $2);
			debug_msg "parsed parameter: $1=$2";
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
			} elsif ($line =~ /^\s*set\s+(\w+)\s*=\s*"?(.*?)"?;$/) {
				$gcfg{$1} = $2;
				debug_msg "parsed setting $1=$2";
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

# Capture all output from all child processes into a pipe so we can
# parse it.
pipe RD, WR;

my @all_output : shared;

my $reader_thr = threads->create(sub {
	{
		close WR;

		lock(@all_output);

		while (<RD>) {
			print STDERR if $opts{v};
			push @all_output, $_;
		}
	}
});

parse_conf($ts_cfg);

# Perform configuration sanity checks.
fatalx "no client(s) specified" unless exists $gcfg{client};
fatalx "no database source (dsn) specified"
    if $opts{R} and !exists $gcfg{dsn};

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

	if (exists $n{args}) {
		if (ref eq "ARRAY") {
		} else {
			$n{args} = [ $n{args} ];
		}
	} else {
		$n{args} = [ ];
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

my @mds_pids;
my @ios_pids;
my @cli_pids;

my $success = 0;

local $SIG{CHLD} = sub { };

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

my $fsuuid = "0x$gcfg{fsuuid}";

my @pids;

my %hosts;

# Checkout the source and build it
foreach my $n (@mds, @ios, @cli) {
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
		$n->{src_dir},
		$n->{data_dir},
		"$n->{base_dir}/tmp",
	);

	my @cmds;

	push @cmds, q[setup_src] unless exists $hosts{$n->{host}};
	$hosts{$n->{host}} = 1;

	if ($n->{type} eq "client") {
		$n->{wok_ctlsock} = "$n->{base_dir}/wok.ctlsock";

		$n->{mp} = "$n->{base_dir}/mp";
		push @mkdir, $n->{mp};

		push @cmds, qq[$sudo dd if=/dev/urandom of=$TSUITE_RANDOM bs=1048576 count=1024];
	}

	my $authbuf_fn = "$n->{data_dir}/authbuf.key";

	my @patch;
	if ($diff) {
		push @patch, <<EOF;
			patch -p0 <<'___PATCH_EOF'
$diff
___PATCH_EOF
EOF
	}

	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n, 1)]}

		setup_src()
		{
			$sudo umount -l -f $n->{data_dir} || true
			# For hosts with multiple services, a client
			# may have previously mounted here, so clear it.
			$sudo umount -l -f $n->{base_dir}/mp || true

			$sudo rm -rf $n->{base_dir}
			$sudo rm -rf $n->{base_dir}/mp
			mkdir -p @mkdir

			cd $n->{src_dir}
			git clone $repo_url .
			./bootstrap.sh

			cat <<'___MKCFG_EOF' > mk/local.mk
@{$gcfg{mkcfg}}
___MKCFG_EOF

			@patch

			make build >/dev/null

			(
				set +x

				echo %PFL_COMMID% \$(git log -n 1 --pretty=format:%H)

				cd slash2
				echo %SL2_COMMID% \$(git log -n 1 --pretty=format:%H)
			)

			touch $authbuf_fn
			cat <<'___AUTHBUF_EOF' > $authbuf_fn
$authbuf
___AUTHBUF_EOF
			sudo chown root $authbuf_fn
			sudo chmod 400 $authbuf_fn
		}

		@cmds
		mkdir -p @mkdir
EOF
}

waitjobs \@pids, $step_timeout;

# Create the MDS file systems
foreach my $n (@mds) {
	debug_msg "initializing slashd environment: $n->{res_name} : $n->{host}";

	my @cmds;
	if (exists $n->{journal}) {
		push @cmds, "$sudo slmkjrnl -f -b $n->{journal} -u $fsuuid";
	} else {
		push @cmds, "$sudo slmkjrnl -f -D $n->{data_dir} -u $fsuuid";
	}

	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}

		$sudo pkill -9 zfs-fuse || true
		$sudo pkill -9 slashd || true
		sleep 5
		$sudo modprobe fuse
		$sudo mkdir -p /var/run/zfs
		runbg $sudo $zfs_fuse
		sleep 2

		@{$n->{fmtcmd}}
		$sudo $zpool destroy $n->{zpool_name} || true
		$sudo $zpool create -f $n->{zpool_name} $n->{zpool_args}
		$sudo $zfs set atime=off $n->{zpool_name}
		$sudo slmkfs -I $n->{site_id}:$n->{id} -u $fsuuid /$n->{zpool_name}
		sync
		# XXX race condition here
		sleep 3
		$sudo umount /$n->{zpool_name}
		$sudo pkill zfs-fuse
		sleep 8

		@cmds
EOF
}

waitjobs \@pids, $step_timeout;

sub daemon_setup {
	my $n = shift;

	return <<EOF;
	run_daemon()
	{
		ulimit -c unlimited
		export PSC_LOG_FILE=$n->{base_dir}/$n->{type}.log
		touch \$PSC_LOG_FILE
		sudo sh -c 'echo %e.core > /proc/sys/kernel/core_pattern'
		local prog=\$1
		while :; do
			set +e
			$sudo "\$@"
			local status=\$?
			set -e
			[ \$status -eq 0 ] && break
			local corefile=\$prog.core
			if [ -e "\$corefile" ]; then
				[ \$status -gt 128 ] && echo exited via signal \$((status - 128))
				tail \$PSC_LOG_FILE
				local cmdfile=$n->{base_dir}/$n->{type}.gdbcmd
				{
					echo set confirm off
					echo set height 0
					echo set width 0
					echo thr ap all bt
				} >\$cmdfile
				sudo gdb -batch -c \$corefile -x \$cmdfile \$(which \$prog) 2>&1 | $n->{src_dir}/tools/filter-pstack
			fi
			sleep 2
		done
	}
EOF
}

$SIG{INT} = sub {
	debug_msg "handling interrupt";
	cleanup();
	exit 1;
};

# Launch MDS servers
foreach my $n (@mds) {
	debug_msg "launching slashd: $n->{res_name} : $n->{host}";

	push @mds_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon slashd -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
EOF
}

foreach my $n (@mds) {
	debug_msg "waiting for slashd online: $n->{res_name} : $n->{host}";

	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		until slmctl -sc >/dev/null 2>&1; do
			sleep 1
		done
EOF
}

waitjobs \@pids, $step_timeout;

# Create the IOS file systems
foreach my $n (@ios) {
	debug_msg "initializing sliod environment: $n->{res_name} : $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{$n->{fmtcmd}}
		$sudo slmkfs -i -u $fsuuid -I $n->{site_id}:$n->{id} $n->{fsroot}
EOF
}

waitjobs \@pids, $step_timeout;

# Launch the IOS servers
foreach my $n (@ios) {
	debug_msg "launching sliod: $n->{res_name} : $n->{host}";

	push @ios_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon sliod -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
EOF
}

# Launch the client mountpoints
foreach my $n (@cli) {
	debug_msg "launching mount_wokfs: $n->{host}";

	$n->{test_src_dir} = "$n->{src_dir}/$TSUITE_REL_BASE/tests/$ts_name/cmd";

	my $args = join ',', @{ $n->{args} },
	    "ctlsock=$n->{ctlsock}",
	    "datadir=$n->{data_dir}",
	    "slcfg=$n->{slcfg}";

	push @cli_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		$sudo modprobe fuse
		run_daemon mount_wokfs -U -L "insert 0 $n->{src_dir}/slash2/mount_slash/slash2.so $args" $n->{mp}
EOF
}

sub test_setup {
	my $n = shift;

	return <<EOF;
	export RANDOM_DATA=$TSUITE_RANDOM
	cd $n->{test_src_dir}
	sudo mkdir -p $n->{mp}/tmp
	sudo chmod 1777 $n->{mp}/tmp

	run_test()
	{
		local test=\$1
		local id=\$2
		local max=\$3

		#export LOCAL_TMP=$n->{basedir}/tmp/\${test%.*}
		export LOCAL_TMP=$n->{mp}/tmp/\${test%.*}
		export SRC=$n->{src_dir}
		rm -rf \$LOCAL_TMP
		mkdir -p \$LOCAL_TMP
		cd \$LOCAL_TMP

		local launcher=
		if [ x"\$test" != x"\${test%.sh}" ]; then
			launcher=\"bash -ue@{[ $opts{v} ? "x" : ""]}\"
		fi

		\$launcher \$test \$id \$max
	}

	convert_ms()
	{
		local s=\${1%.*}
		local ns=\$(echo \${1#*.} | sed 's/^0*//')
		echo \$((s * 1000 + ns / 1000000))
	}

	run_timed_test()
	{
		local test=\$1
		local id=\$2

		local time0=\$(date +%s.%N)
		run_test \$@
		local time1=\$(date +%s.%N)
		local time0_ms=\$(convert_ms \$time0)
		local time1_ms=\$(convert_ms \$time1)
		echo %TSUITE_RESULT% \$test:\$id \$((time1_ms - time0_ms))
	}

	_dep_guts()
	{
		case \$1 in
		iozone)	cd $n->{src_dir}/distrib/iozone
			make linux-AMD64
			;;
		*)	die "unhandled dependency \$i"
			;;
		esac
	}
	export -f _dep_guts

	dep()
	{
		local prog

		for prog; do
			case \$prog in
			iozone)	addpath $n->{src_dir}/distrib/iozone ;;
			sft)	addpath $n->{src_dir}/sft ;;
			esac

			hasprog \$prog && continue

			(_dep_guts \$prog)
		done
	}
	export -f dep
EOF
}

# Set 1: run the client application tests, serially, measuring stats on
# each so we can present historical performance analysis.
foreach my $n (@cli) {
	debug_msg "client stage 1: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}

		until mount | grep -q $n->{mp}; do
			sleep 1
		done

		@{[test_setup($n)]}

		for test in *; do
			run_timed_test $n->{test_src_dir}/\$test $n->{id} $nclients
		done
EOF
}

waitjobs \@pids, $total_timeout;

# Set 2: run all tests in parallel without faults for performance
# regressions and exercise.
foreach my $n (@cli) {
	debug_msg "client stage 2: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		run_all_tests()
		{
			for test in *; do
				echo launching test from all: \$test
				run_test $n->{test_src_dir}/\$test $n->{id} $nclients &
			done
			wait
		}

		run_timed_test run_all_tests $n->{id} $nclients
EOF
}

waitjobs \@pids, $total_timeout;

# Set 3: now run the entire suite again, injecting faults at random
# places to test failure tolerance.
foreach my $n (@cli) {
	debug_msg "client stage 3: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		for test in *; do
			run_test $n->{test_src_dir}/\$test $n->{id} $nclients
		done
EOF
}

sub add_fault {
	my ($ra, $n, $name) = @_;

	push @$ra, {
		cmd => "$n->{ctlcmd} -p faults.$name.count+=1",
		host => $n->{host},
	};
}

my @misfortune;

#foreach $n (@cli) {
#	push @misfortune, {
#		cmd => "$wokctl reload 0",
#		node => $n,
#	};
#	add_fault(\@misfortune, $n, 'slash2/request_timeout');
#	add_fault(\@misfortune, $n, 'slash2/read_cb');
#	add_fault(\@misfortune, $n, 'slash2/readrpc_offline');
#}

foreach my $n (@mds) {
	push @misfortune, {
		cmd => "$sudo kill -HUP \$(daemon_pid)",
		node => $n,
	};
}

foreach my $n (@ios) {
	push @misfortune, {
		cmd => "$sudo kill -HUP \$(daemon_pid)",
		node => $n,
	};
#	add_fault(\@misfortune, $n, 'sliod/fsio_read_fail');
#	add_fault(\@misfortune, $n, 'sliod/crcup_fail');
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
	push @killpid, runcmd "$ssh $a->{node}{host} bash", <<EOF;
		@{[init_env($a->{node})]}
		$a->{cmd}
EOF
	waitjobs \@killpid, $step_timeout;

} while (waitjobs \@pids, $total_timeout, WF_NONBLOCK);

debug_msg "completed test suite successfully";

$success = 1;

}; # end of eval

my $emsg = $@;

if ($emsg) {
	debug_msg "error encountered: $emsg";

	# Give some time for the daemons to be examined for coredumps...
	sleep 10;
}

sub cleanup {
	my $n;
	foreach $n (@cli) {
		debug_msg "unmounting mount_wokfs: $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo umount -l -f $n->{mp}
EOF
	}

	foreach $n (@ios) {
		debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			slictl stop
EOF
	}

	foreach $n (@mds) {
		debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			slmctl stop
EOF
	}

	kill 'HUP', @cli_pids, @ios_pids, @mds_pids;

	waitjobs \@cli_pids, $step_timeout, WF_NONFATAL;
	waitjobs \@ios_pids, $step_timeout, WF_NONFATAL;
	waitjobs \@mds_pids, $step_timeout, WF_NONFATAL;

	foreach $n (@cli) {
		debug_msg "killing mount_wokfs: $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 mount_wokfs
EOF
	}

	foreach $n (@ios) {
		debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 sliod
EOF
	}

	foreach $n (@mds) {
		debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
		runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 slashd
EOF
	}
}

cleanup();

close WR;

$reader_thr->join();

lock @all_output;

my $output = join '', @all_output;

my $pfl_commid = $output =~ /^%PFL_COMMID% (\S+?)/m;
my $sl2_commid = $output =~ /^%SL2_COMMID% (\S+?)/m;

sub parse_results {
	my ($output) = @_;

	my @out;
	foreach my $result ($output =~
	    /^%TSUITE_RESULT% (.*)$/gm) {
		$result =~ m!^.*?([^:/]+):(\d+) (\d+)$! or next;
		push @out, {
			name		=> $1,
			task_id		=> $2,
			duration	=> $3,
		};
	}

	return @out;
}

# Record results to database to track/analyze historical performance.
if ($opts{R}) {
	debug_msg "parsing output $output";

	my @results = parse_results($output);

	debug_msg "collecting results: ", Dumper(@results);

	my @keys = qw(dsn);
	for my $arg (qw(db_user db_pass)) {
		push @keys, $arg if exists $gcfg{$arg};
	}
	my @args = @gcfg{@keys};

	my $dbh = DBI->connect(@args);
	unless ($dbh) {
		$gcfg{db_pass} = "YES" if exists $gcfg{db_pass};
		my @safe_args = @gcfg{@keys};
		fatal "unable to connect to database (@safe_args): $DBI::errstr";
	}

	my @param;
	push @param, $ts_name;			# $1
	push @param, $ts_user;			# $2
	push @param, $diff;			# $3
	push @param, $success;			# $4
	push @param, $output;			# $5
	push @param, $sl2_commid;		# $6
	push @param, $pfl_commid;		# $7

	$dbh->do("BEGIN");

	my $query = <<'SQL';
		INSERT INTO s2ts_run (
			suite_name,
			launch_date,
			user,
			diff,
			status,
			output,
			sl2_commid,
			pfl_commid
		) VALUES (
			?,			-- $1: suite_name
			CURRENT_TIMESTAMP,	--     launch_date
			?,			-- $2: user
			?,			-- $3: diff
			?,			-- $4: status
			?,			-- $5: output
			?,			-- $6: sl2_commid
			?			-- $7: pfl_commid
		)
SQL
	$dbh->do($query, {}, @param)
	    or die "failed to execute query; query=$query; " .
		   "params=@param; error=$DBI::errstr";

	my $run_id = $dbh->last_insert_id("", "", "", "");

	my $sth = $dbh->prepare(<<SQL);
		INSERT INTO s2ts_result (
			run_id,
			test_name,
			task_id,
			duration_ms
		) VALUES (
			?,
			?,
			?,
			?
		)
SQL
	foreach my $r (@results) {
		my @params = ($run_id, @{$r}{qw(name task_id duration)});
		$sth->execute(@params)
		    or die "failed to execute query; params=@params; " .
			   "error=$DBI::errstr";
	}

	$query = "COMMIT";
	$dbh->do($query) or die "failed to execute query; " .
	    "query=$query; error=$DBI::errstr";

	$dbh->disconnect;
} else {
	debug_msg "not collecting results";
}

# Send e-mail report.
if ($opts{m}) {
	debug_msg "sending e-mail report";

	my $smtp = Net::SMTP->new('mailer.psc.edu');

	my $to = 'slash2-devel+report@psc.edu';
	my $from = 'slash2-devel@psc.edu';

	# Trim output to last N lines.
	my $index = min(75, @all_output);
	my $output_small = splice @all_output, -$index;

	$smtp->mail($from);
	$smtp->to($to);
	$smtp->data();
	$smtp->datasend(<<EOM);
To: $to
From: $from
Subject: tsuite report

Launched by user: $ts_user

@{[$emsg ? <<__EOE : ""]}
------------------------------------------------------------------------
error: $emsg
------------------------------------------------------------------------
__EOE

$output_small

EOM
	$smtp->dataend();
	$smtp->quit;
} else {
	debug_msg "not sending e-mail report";

	warn "$output\n\nerror: $emsg\n" if $emsg;
	exit 1 if $emsg;
}
