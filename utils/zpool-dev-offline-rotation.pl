#!/usr/bin/env perl
# $Id: zpool-dev-offline-rotation.pl 24976 2014-12-12 22:42:58Z yanovich $

use POSIX;
use Getopt::Std;
use strict;
use warnings;

$ENV{PATH} .= ":/local/sbin/";

my %opts;
my $send_mail = 0;
my $from = 'hal+mon@illusion2.psc.edu';
my @mail_q;

sub usage {
	die "usage: $0 [-nv] [-M email] pool\n";
}

sub vprint {
	if ($opts{M}) {
		local $" = '';
		push @mail_q, "@_\n";
	} else {
		warn "@_\n" if $opts{v};
	}
}

sub my_exit {
	if ($send_mail && $opts{M}) {
		open M, " | sendmail -t" or die "$!";
		print M <<EOM;
To: $opts{M}
From: $from
Subject: $0 status

EOM
		print M @mail_q;
		close M;
	}
	exit;
}

getopts("M:nv", \%opts) or usage;
usage unless @ARGV == 1;
my $pool = $ARGV[0];

my $noffline = 0;
my $degraded = 0;
my %disks;
my @disks;
foreach my $ln (`zpool status \Q$pool\E`) {
	$ln =~ m,^\s+(disk/by-id/.*?)\s+(\w+), or next;
	my $diskname = $1;
	$diskname = "/dev/$diskname" unless $diskname =~ m!/dev!;
	my $state = $2;
	$disks{$diskname} = {
		idx => scalar @disks,
		state => $state,
	};
	vprint "saw disk $diskname state $state";
	push @disks, $diskname;
	if ($state ne "ONLINE" and
	    $state ne "OFFLINE") {
		$degraded = 1;
	}
	$degraded = 1 if $ln =~ /resilvering/;
	$noffline++ if $state eq "OFFLINE";
}

if ($noffline > 1 || $degraded) {
	vprint "pool unhealthy; disabling rotation";
	while (my ($diskname, $disk) = each %disks) {
		toggle_disk $diskname if $disk->{state} eq "OFFLINE";
	}
}

sub toggle_disk {
	my $diskname = shift;
	vprint "attempting to toggle $diskname";
	die "$diskname: invalid disk!\n" unless $disks{$diskname};
	my %trans = (
		ONLINE => "offline",
		OFFLINE => "online",
	);
	my $d = $disks{$diskname};
	my $new_state = $trans{$d->{state}};
	my $cmd;
	if ($new_state eq "offline") {
		# If this disk is online, we would be turning it off.
		# So instead, offline the next disk to achieve round
		# robin.
		my $next = $disks[($d->{idx} + 1) % @disks];
		$cmd = "zpool offline -t \Q$pool\E $next\n";
	} else {
		$cmd = "zpool $new_state \Q$pool\E $diskname\n";
	}
	vprint "running: $cmd";
	vprint `$cmd` unless $opts{n};
	$send_mail = 1;
}

vprint;

use constant YR		=> 0;
use constant MON	=> 1;
use constant DAY	=> 2;
use constant HR		=> 3;
use constant MIN	=> 4;
use constant SEC	=> 5;
use constant DISKNAME	=> 6;

my $last_rec;
foreach my $ln (`zpool history \Q$pool\E`) {
	my @t = $ln =~ /^(\d\d\d\d)-(\d\d)-(\d\d)\.(\d\d):(\d\d):(\d\d) zpool (?:online|offline -t) \Q$pool\E (.*)/;
	$last_rec = \@t if @t;
}

unless ($last_rec) {
	toggle_disk($disks[0]);
	my_exit();
}

$last_rec->[YR] -= 1900;
$last_rec->[MON]--;

my $last_tm = POSIX::mktime(@$last_rec[SEC,MIN,HR,DAY,MON,YR]);
my $now = time;

use constant INTV => 12 * 60 * 60 - 30;

my $ago = $now - $last_tm;
my $ago_hrs = int($ago / 60 / 60);
my $ago_mins = int(($ago % 3600) / 60);
my $ago_secs = $ago % 60;
my $s_ago = "${ago_hrs}h${ago_mins}m${ago_secs}s";
vprint "now is $now; last toggle was $last_tm ($s_ago ago)\n";

toggle_disk($last_rec->[DISKNAME]) if $last_tm + INTV < $now;
my_exit();
