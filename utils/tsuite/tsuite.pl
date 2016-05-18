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

# TODO
# - track total CPU time
# - track total RAM use
# - track total net IOPS
# - check hash table stats

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

my $pfl_sudo = "command sudo env PSC_DUMPSTACK=1";

sub usage {
	die <<EOF
usage: $0 [-BCmPRv] [-c commspec] [-D descr] [-u user] [suite-name [test ...]]

options:
  -B		whether to use any configured SSH bounce host
  -C		do not run teardown/cleanup
  -c commspec	run against the specified commit/revision
  -D descr	optional description to include in report
  -m		send e-mail report
  -P		do not incorporate local patches in the run
  -R		record results to database
  -u user	override user for SSH connection establishment
  -v		verbose (debugging) output

If any are specified, all such `test' arguments are interpretted as
/bin/sh patterns to match a subset of the individual tests.

`commspec' is the form `repo:commid,...' e.g.
`slash2:abc123,pfl:def456'.

EOF
}

getopts("BCc:D:mPRu:v", \%opts) or usage();

my $ssh_user = "";
$ssh_user = " -l $opts{u} " if $opts{u};

my $ts_dir = dirname($0);
my $ts_name = "suite0";
my $ts_user = `id -un`;
chomp $ts_user;
$ts_name = shift @ARGV if @ARGV > 0;
my $which_tests = @ARGV ? join(' ', @ARGV) : '*';

sub min($$) {
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
				$pfl_sudo $n->{src_dir}/slash2/msctl/msctl -S $n->{ctlsock} \$@
			}
			export -f msctl

			wokctl()
			{
				$pfl_sudo $n->{src_dir}/wokfs/wokctl/wokctl -S $n->{wok_ctlsock} \$@
			}
			export -f wokctl
EOF
	} elsif ($n->{type} eq "mds") {
		$n->{ctlcmd} = "slmctl";
		push @cmd, <<EOF;
			slmctl()
			{
				$pfl_sudo $n->{src_dir}/slash2/slmctl/slmctl -S $n->{ctlsock} \$@
			}
			export -f slmctl
EOF
	} else {
		$n->{ctlcmd} = "slictl";
		push @cmd, <<EOF;
			slictl()
			{
				$pfl_sudo $n->{src_dir}/slash2/slictl/slictl -S $n->{ctlsock} \$@
			}
			export -f slictl
EOF
	}

	my @env_vars;
	while (my ($k, $v) = each %$n) {
		push @env_vars, "$k='$v' " if $k =~ /^[A-Z]/;;
	}

	return <<EOF;
	set -e
	set -u

	_make_ps4()
	{
		local host=\$1
		local line=\$2
		local timestamp=\$3

		local n=\$((SHLVL - 2))
		printf "%\${n}s[%s:%d:%d %d] " "" \$host \$\$ \$timestamp \$line
	}
	export -f _make_ps4

	PS4='\$(_make_ps4 "\\h" \$LINENO "\\D{%s}")'
	export PS4

	die()
	{
		echo \$@ >&2
		exit 1
	}
	export -f die

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

	min_sysctl()
	{
		hasprog sysctl || return

		local param=\$1
		local minval=\$2

		local v=\$(sysctl -n \$param)
		[ \$v -lt \$minval ] && sudo sysctl \$param=\$minval
	}
	export -f min_sysctl

	tsuite_wget()
	{
		local size=\$1
		local md5=\$2
		local sha256=\$3
		local url=\$4
		local fn=\$(basename \$url)

		exclude_time_start
		if [ -e "$n->{TMPDIR}/\$fn" ]; then
			cp "$n->{TMPDIR}/\$fn" .
		else
			wget -nv \$url
			cp \$fn "$n->{TMPDIR}/"
		fi
		exclude_time_end

		(
			echo \$md5 \$fn | md5sum -c
			echo \$sha256 \$fn | sha256sum -c
		) || (rm -f "$n->{TMPDIR}/\$fn" && false)
	}
	export -f tsuite_wget

	tsuite_decompress()
	{
		case \${1##*.} in
		bz2)	if hasprog pbunzip2; then
				pbunzip2 -c \$1
			else
				bunzip2 -c \$1
			fi ;;
		gz)	if hasprog pigz; then
				pigz -dc \$1
			else
				gunzip -c \$1
			fi ;;
		xz)	if hasprog pixz; then
				pixz -d -t < \$1
			else
				xz -dc \$1
			fi ;;
		*)	die "unknown suffix: \$1";
		esac
	}
	export -f tsuite_decompress

	mkdir_recurse()
	{
		# GNU /bin/mkdir performs a check instead of blindly
		# trying to create and ignoring EEXIST, which is prone
		# to races.
		mkdir -p "\$1" || :
		[ -d "\$1" ]
	}
	export -f mkdir_recurse

	addpath /sbin
	addpath $n->{src_dir}/slash2/slashd
	addpath $n->{src_dir}/slash2/sliod
	addpath $n->{src_dir}/slash2/slkeymgt
	addpath $n->{src_dir}/slash2/slmkfs
	addpath $n->{src_dir}/slash2/slmkjrnl
	addpath $n->{src_dir}/slash2/utils
	addpath $n->{src_dir}/wokfs/mount_wokfs
	addpath $n->{src_dir}/wokfs/wokctl

	nproc=\$(nproc)
	@{[ $opts{v} ? "set -x" : "" ]}

	data_dir=$n->{data_dir}

	export MAKEFLAGS=-j\$nproc
	export SCONSFLAGS=-j\$nproc
	export PSC_DUMPSTACK=1 @env_vars

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

	fatalx "no pids specified" unless @$pids;

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

	debug_msg "waiting on [@$pids] complete";

	@$pids = ();
}

sub runcmd {
	my ($cmd, $in) = @_;

	debug_msg "launching: $cmd";

	my $infh;
	my $pid = open3($infh, ">&WR", ">&WR", $cmd);
	print $infh "echo debug: launching command remote pid \$\$ to local pid $pid\n", $in;
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

# Parse configuration for MDS and IOS resources.
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
my $diff = "";
$diff = join '', `make scm-diff | grep -v ^index` unless $opts{P};

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
my $total_timeout = 60 * 60 * 28;	# entire client run duration

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
				$cmd =~ s/^/\n$pfl_sudo /;
			}
		} else {
			$n->{fmtcmd} = [ "$pfl_sudo " . $n->{fmtcmd} ];
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

$SIG{CHLD} = sub { };

eval {

$SIG{ALRM} = sub { fatal "timeout exceeded" };

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

my %setup_src;

debug_msg "obtaining source code";

my @comm_checkout;
foreach my $commspec (split /,/, $opts{c} || "") {
	my ($dir, $comm) = split /:/, $commspec
	    or fatal "invalid format: -c $opts{c}";
	$dir = "." if $dir eq "pfl";
	push @comm_checkout, "(cd $dir && git checkout $comm)\n";
}

my $src_done_fn = ".src.done." . time();

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

	if (exists $setup_src{$n->{host}}) {
		push @cmds, q[(wait_until_src)];
	} else {
		push @cmds, q[setup_src];
		$setup_src{$n->{host}} = 1;
	}

	if ($n->{type} eq "client") {
		$n->{wok_ctlsock} = "$n->{base_dir}/wok.ctlsock";

		$n->{mp} = "$n->{base_dir}/mp";
		push @mkdir, $n->{mp};

		push @cmds, qq[find $TSUITE_RANDOM -size +1073741823c | grep $TSUITE_RANDOM || $sudo dd if=/dev/urandom of=$TSUITE_RANDOM bs=1048576 count=1024];
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

			$sudo rm -rf $n->{base_dir}/mp
			$sudo rm -rf $n->{base_dir}
			mkdir -p @mkdir

			cd $n->{src_dir}
			sleep \$((RANDOM % 10))
			git clone $repo_url .
			./bootstrap.sh

			@comm_checkout

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
			$sudo chown root $authbuf_fn
			$sudo chmod 400 $authbuf_fn
			touch $src_done_fn
		}

		wait_until_src()
		{
			set +x
			until [ -e $n->{src_dir}/$src_done_fn ]; do
				sleep 1
			done
		}

		@{[join "\n", @cmds]}
		mkdir -p @mkdir
EOF
}

waitjobs \@pids, $step_timeout;

# Create the MDS file system(s).
foreach my $n (@mds) {
	debug_msg "initializing slashd environment: $n->{res_name} : $n->{host}";

	my @cmds;
	if (exists $n->{journal}) {
		push @cmds, "$pfl_sudo slmkjrnl -f -b $n->{journal} -u $fsuuid";
	} else {
		push @cmds, "$pfl_sudo slmkjrnl -f -D $n->{data_dir} -u $fsuuid";
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
		$pfl_sudo slmkfs -I $n->{site_id}:$n->{id} -u $fsuuid /$n->{zpool_name}
		sync
		# XXX race condition here
		sleep 3
		$sudo umount /$n->{zpool_name}
		$sudo pkill zfs-fuse
		sleep 8
		$sudo rm -f /dev/shm/upsch.*

		@cmds
EOF
}

waitjobs \@pids, $step_timeout;

sub daemon_setup {
	my $n = shift;

	return <<EOF;
	run_daemon()
	{
		local OPTIND c

		once=0
		while getopts "O" c; do
			case \$c in
			O) once=1;;
			*) die "unknown option: \$c";;
			esac
		done
		shift \$((OPTIND - 1))

		$sudo sysctl vm.overcommit_memory=1
		ulimit -c unlimited
		$sudo sh -c 'echo %e.core > /proc/sys/kernel/core_pattern'
		local prog=\$1
		: \${PSC_LOG_FILE:=$n->{base_dir}/$n->{type}.log}
		while :; do
			set +e
			$sudo pkill -9 \$1 && sleep 3 || :
			$sudo env ASAN_SYMBOLIZER_PATH=\$(which llvm-symbolizer) ASAN_OPTIONS=symbolize=1 PSC_LOG_FILE=\$PSC_LOG_FILE "\$@"
			local status=\$?
			set -e
			[ \$status -gt 128 ] && echo exited via signal \$((status - 128))

			# A hard kill means another instance of tsuite
			# has asked us to quietly go away.
			[ \$status -eq 137 ] && break

			[ \$status -eq 0 ] && break
			local corefile=\$prog.core
			if [ -e "\$corefile" ]; then
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
			[ \$once -eq 1 ] && break
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

# Launch MDS servers.
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

# Create the IOS file systems.
foreach my $n (@ios) {
	debug_msg "initializing sliod environment: $n->{res_name} : $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{$n->{fmtcmd}}
		$pfl_sudo slmkfs -i -u $fsuuid -I $n->{site_id}:$n->{id} $n->{fsroot}
EOF
}

waitjobs \@pids, $step_timeout;

# Launch the IOS servers.
foreach my $n (@ios) {
	debug_msg "launching sliod: $n->{res_name} : $n->{host}";

	push @ios_pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[daemon_setup($n)]}
		run_daemon sliod -S $n->{ctlsock} -f $n->{slcfg} -D $n->{data_dir}
EOF
}

# Launch the client mountpoints.
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
		run_daemon -O mount_wokfs -U -L "insert 0 $n->{src_dir}/slash2/mount_slash/slash2client.so $args" $n->{mp}
EOF
}

if (exists $gcfg{testenv}) {
	if (ref $gcfg{testenv} eq "ARRAY") {
	} else {
		$gcfg{testenv} = [ $gcfg{testenv} ];
	}
} else {
	$gcfg{testenv} = [ ];
}

sub test_setup {
	my $n = shift;

	return <<EOF;
	export RANDOM_DATA=$TSUITE_RANDOM
	@{[map { "export $_\n" } @{ $gcfg{testenv} }]}

	cd $n->{test_src_dir}
	sudo mkdir -p "$n->{mp}/tmp" || :
	sudo chmod 1777 "$n->{mp}/tmp"

	run_test()
	{
		local test=\$1
		local id=\$2
		local max=\$3

		local testrpath=\$id/\${test##*/}
		export LOCAL_TMP=\$TMPDIR/tsuite.\$RANDOM/\$testrpath
		local testdir=$n->{mp}/tmp/\$id/\${test##*/}
		export SRC=$n->{src_dir}

		rm -rf "\$LOCAL_TMP"
		mkdir_recurse "\$LOCAL_TMP"

		rm -rf "\$testdir"
		mkdir_recurse "\$testdir"
		cd "\$testdir"

		local launcher=
		if [ x"\$test" != x"\${test%.sh}" ]; then
			launcher='bash -eu@{[ $opts{v} ? "x" : ""]}'
		fi

		TMPDIR=\$testdir \$launcher \$test \$id \$max && rm -rf \$testdir
	}

	convert_ms()
	{
		local s=\${1%.*}
		local ns=\$(echo \${1#*.} | sed 's/^0*//')
		echo \$((s * 1000 + ns / 1000000))
	}
	export -f convert_ms

	run_timed_test()
	{
		local OPTIND

		local dont_exclude=0
		while getopts "X" c; do
			case \$c in
			X) dont_exclude=1;;
			esac
		done
		shift \$((OPTIND - 1))

		local test=\$1
		local id=\$2

		local time0=\$(date +%s.%N)
		run_test \$@
		local time1=\$(date +%s.%N)
		local time0_ms=\$(convert_ms \$time0)
		local time1_ms=\$(convert_ms \$time1)
		[ \$dont_exclude -eq 1 ] && _EXCLUDE_TIME_MS=0
		echo %TSUITE_RESULT% \$test:\$id \$((time1_ms - time0_ms - _EXCLUDE_TIME_MS))
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

	exclude_time_start()
	{
		_EXCLUDE_TIME0=\$(date +%s.%N)
	}
	export -f exclude_time_start

	_EXCLUDE_TIME_MS=0
	exclude_time_end()
	{
		local time1=\$(date +%s.%N)
		local time0_ms=\$(convert_ms \$_EXCLUDE_TIME0)
		local time1_ms=\$(convert_ms \$time1)
		_EXCLUDE_TIME_MS+=\$((time1_ms - time0_ms))
	}
	export -f exclude_time_end

#	sleep()
#	{
#		exclude_time_start()
#		command sleep \$@
#		exclude_time_end()
#	}

	shopt -s extglob
EOF
}

# Give IOS a moment to connect.
sleep(8);

# Set 1: run the client application tests, serially, measuring stats on
# each so we can present historical performance analysis.
foreach my $n (@cli) {
	debug_msg "client stage 1: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}

		wait_for_mds()
		{
			set +x

			while :; do
				mds=\$($n->{ctlcmd} -Hp sys.mds | awk '{print \$2}' | sed 's/\\(.*\\)@\\(.*\\)/\\2.\\1/')
				[ -n "\$mds" ] && break
				sleep 1
			done

			df $n->{mp}

			until $n->{ctlcmd} -p sys.resources.\$mds.connected | grep 1; do
				sleep 1
			done
		}

		(wait_for_mds)

		@{[test_setup($n)]}

		for test in $which_tests; do
			run_timed_test $n->{test_src_dir}/\$test $n->{id} $nclients
		done
EOF
}

waitjobs \@pids, $total_timeout;

goto SKIP_FAIL if $which_tests ne "*";

# Set 2: run all tests in parallel without faults for performance
# regressions and exercise.
foreach my $n (@cli) {
	debug_msg "client stage 2: $n->{host}";
	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
		@{[init_env($n)]}
		@{[test_setup($n)]}

		run_all_tests()
		{
			for test in $n->{test_src_dir}/*; do
				run_test \$test $n->{id} $nclients &
			done
			wait
		}

		run_timed_test -X run_all_tests $n->{id} $nclients
EOF
}

waitjobs \@pids, $total_timeout;

# foreach my $n (@mds, @ios) {
#	debug_msg "ensuring no errors: $n->{host}";
#	push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
#		@{[init_env($n)]}
#		slictl errors
# EOF
# }

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

SKIP_FAIL:

debug_msg "completed test suite successfully";

$success = 1;

}; # end of eval

my $emsg = $@;

if ($emsg) {
	debug_msg "error encountered: $emsg";

	# Give some time for the daemons to be examined for coredumps...
	sleep 20;
}

sub cleanup {
	return if $opts{C};

	debug_msg "running cleanup";

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

	my @pids = (@cli_pids, @ios_pids, @mds_pids);
	kill 'HUP', @pids;

	waitjobs \@pids, $step_timeout, WF_NONFATAL;

	foreach $n (@cli) {
		debug_msg "killing mount_wokfs: $n->{host}";
		push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 mount_wokfs
EOF
	}

	foreach $n (@ios) {
		debug_msg "stopping sliod: $n->{res_name} : $n->{host}";
		push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 sliod
EOF
	}

	foreach $n (@mds) {
		debug_msg "stopping slashd: $n->{res_name} : $n->{host}";
		push @pids, runcmd "$ssh $n->{host} bash", <<EOF;
			@{[init_env($n)]}
			$sudo pkill -9 slashd
EOF
	}

	waitjobs \@pids, $step_timeout, WF_NONFATAL;
}

$SIG{INT} = 'IGNORE';
cleanup();
#$SIG{INT} = 'DEFAULT';

close WR;

debug_msg "waiting for reader thread";

$reader_thr->join();

debug_msg "examining output";

lock @all_output;

my $output = join '', @all_output;

my ($pfl_commid) = $output =~ /^%PFL_COMMID% (\S+)/m;
my ($sl2_commid) = $output =~ /^%SL2_COMMID% (\S+)/m;

sub parse_results {
	my ($output) = @_;

	my %out;
	foreach my $result ($output =~
	    /^%TSUITE_RESULT% (.*)$/gm) {
		$result =~ m!^.*?([^:/]+):(\d+) (\d+)$! or next;
		$out{"$1:$2"} = {
			name		=> $1,
			task_id		=> $2,
			duration_ms	=> $3,
		};
	}

	return %out;
}

my %results = parse_results($output);
my $run_id;

# Record results to database to track/analyze historical performance.
if ($opts{R}) {
	debug_msg "parsing output $output";

	$output = "" if $success;

	debug_msg "collecting results: ", Dumper(\%results);

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
	push @param, $opts{D} || "";		# $1
	push @param, $ts_name;			# $2
	push @param, $ts_user;			# $3
	push @param, $diff;			# $4
	push @param, $success;			# $5
	push @param, $output;			# $6
	push @param, $sl2_commid;		# $7
	push @param, $pfl_commid;		# $8

	$dbh->do("BEGIN");

	my $query = <<'SQL';
		INSERT INTO s2ts_run (
			descr,			-- 1
			suite_name,		-- 2
			launch_date,		--
			user,			-- 3
			diff,			-- 4
			success,		-- 5
			output,			-- 6
			sl2_commid,		-- 7
			pfl_commid		-- 8
		) VALUES (
			?,			-- $1: descr
			?,			-- $2: suite_name
			CURRENT_TIMESTAMP,	--     launch_date
			?,			-- $3: user
			?,			-- $4: diff
			?,			-- $5: success
			?,			-- $6: output
			?,			-- $7: sl2_commid
			?			-- $8: pfl_commid
		)
SQL
	$dbh->do($query, {}, @param)
	    or die "failed to execute query; query=$query; " .
		   "params=@param; error=$DBI::errstr";

	$run_id = $dbh->last_insert_id("", "", "", "");

	if ($success) {
		$query = <<SQL;
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
		my $sth = $dbh->prepare($query)
		    or die "failed to prepare query; query=$query";

		while (my (undef, $r) = each %results) {
			my @params = ($run_id, @{$r}{qw(name task_id duration_ms)});
			$sth->execute(@params)
			    or die "failed to execute query; params=@params; " .
				   "error=$DBI::errstr";
		}
	}

	$query = "COMMIT";
	$dbh->do($query) or die "failed to execute query; " .
	    "query=$query; error=$DBI::errstr";

	if ($opts{m} && $success) {
		# Sending an e-mail report: pull out previous runs and
		# compute an historical average for comparison.
		my $sth = $dbh->prepare(<<SQL)
		SELECT	*,
			(
				SELECT	AVG(r2.duration_ms)
				FROM	s2ts_result r2,
					s2ts_run run
				WHERE	r2.task_id = res.task_id
				  AND	r2.test_name = res.test_name
				  AND	r2.run_id = run.id
				  AND	run.success
				  AND	run.diff = ''
				  AND	r2.run_id != res.run_id
			) AS havg_ms
		FROM	s2ts_result res
		WHERE	run_id = ?
		ORDER BY
			test_name ASC
SQL
		    or die "unable to execute SQL; err=$DBI::errstr";
		$sth->execute($run_id) or die "unable to execute SQL; err=$DBI::errstr";
		while (my $tr = $sth->fetchrow_hashref) {
			my $r = $results{"$tr->{test_name}:$tr->{task_id}"};
			$r->{havg_ms} = $tr->{havg_ms};
			$r->{delta} = $r->{duration_ms} - $r->{havg_ms};
			$r->{pct_change} = $r->{delta} / $r->{havg_ms} * 100;
		}
	}

	$dbh->disconnect;
} else {
	debug_msg "not collecting results";
}

# Send e-mail report.
if ($opts{m}) {
	debug_msg "sending e-mail report";

	my $smtp = Net::SMTP->new('mailer.psc.edu');

	my $to = 'slash2-devel@psc.edu';
	my $from = 'slash2-devel@psc.edu';

	my $body = "";
	if ($emsg) {
		my $body = <<EOE;
Error encountered:
$emsg
EOE

		# Trim output to last N lines.
		my $index = min(300, @all_output);
		$body .= join '', @all_output[$index .. -1];
	} else {
		$body .= "(all times in millisec)\n";
		$body .= sprintf "%-26s %1s %9s %9s %9s %9s\n",
		    qw(name t histavg duration delta %change);
		$body .= "-" x 72 . "\n";
		foreach my $key (sort { $a cmp $b } keys %results) {
			my $r = $results{$key};
			foreach my $key (qw(havg_ms delta pct_change)) {
				$r->{$key} = 0 unless exists $r->{$key};
			}
			# Expect normal fluctuation.
			# next if $r->{pct_change} < .05;
			$r->{change} = sprintf "%.2f%%", $r->{pct_change};
			$r->{change} = "+$r->{change}" unless $r->{change} =~ /^-/;
			$body .= sprintf "%-26s %1d %9.2f %9.2f %9.2f %9s\n",
			    @{$r}{qw(name task_id havg_ms duration_ms delta
				change)};
		}
	}

	$smtp->mail($from);
	$smtp->to($to);
	$smtp->data();
	$smtp->datasend(<<EOM);
To: $to
From: $from
Subject: tsuite report

@{[defined($run_id) ? "URL: http://lime/results.pl?action=view;id=$run_id\n" : "" ]
}Launched by user: $ts_user
@{[defined($pfl_commid) ? "PFL commit ID: $pfl_commid\n" : "" ]
}@{[defined($sl2_commid) ? "SLASH2 commit ID: $sl2_commid\n" : "" ]
}Status: @{[$success ? "OK": "FAIL"]}
Stock: @{[$diff ? "no" : "yes"]}
@{[$opts{D} ? "Description: $opts{D}" : "" ]
}------------------------------------------------------------------------

$body
EOM
	$smtp->dataend();
	$smtp->quit;
} else {
	debug_msg "not sending e-mail report";

	warn "$output\n\nerror: $emsg\n" if $emsg;
	exit 1 if $emsg;
}
