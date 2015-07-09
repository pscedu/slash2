#!/usr/bin/perl -W
# $Id$

use Getopt::Std;
use strict;
use threads::shared;
use warnings;
use bignum;

my %opts;
my @ios;
my %cfg;
my $syslog_filesize		= 999999999999;
my $syslog_readthr;
my $statfs :shared;
my %fids :shared;		# list of files with multiple replicas
my $fid_lru_head;
my $fid_lru_tail;

sub read_conf {
	my $cfgfn = shift;
	open CFG or die "open $cfgfn: $!\n";
	while (<CFG>) {
		my ($k, $v) = split /\s+/, $_, 1;
		$cfg{$k} = $v;
	}
	close CFG;
}

sub lru_retail {
	$f->{next} if $f->{next};
	$f->{next} =
	$f->{prev} = @fids;
}

sub read_syslog {
	$SIG{'KILL'} = sub { threads->exit(); };
	open SL, "tail -n +0 -f \Q$cfg{syslog}\E |"
	    or die "open $cfg{syslog}: $!\n";
	while (<SL>) {
		# <30>1 2015-06-30T04:37:02-04:00 bl0.psc.teragrid.org
		# mount_slash-dsc 168657 - -
		# [1435653422:681264 msfsthr22:7f70a9fbb700:info main.c mslfsop_release 2260]
		# file closed fid=0x0000000018d16e5b uid=50863 gid=15336 fsize=6795
		# oatime=1432313046:416406351 mtime=1432313048:631661538 sessid=1118980
		# otime=1435653422:657556064 rd=6795 wr=0 prog=/bin/bash
		if (my $fid = / file closed fid=0x0*([a-f0-9]+) /) {
			lock %fids;

			next unless exists $fids{$fid};
			lru_retail($fids{$fid});

		} elsif ($fid = / replicated 0x0*([a-f0-9]+) /) {
			lock %fids;

			next if exists $fids{$fid};
			my $f = $fids{$fid} = {
				fid => $fid,
			};
			lru_retail($f);
			$f->{idx} = @fids;
		}
	}
	close SL;
}

sub scanfs {
	open P, "msctl -R repl-status \Q$cfg{fsroot}\E |" or die "msctl: $!\n";
	while (<P>) {
	}
	close P;
}

sub reload_syslog {
	$syslog_readthr->kill('KILL')->detach() if $syslog_readthr;
	$syslog_readthr = threads->create('read_syslog');

	# XXX There is a race if we send a KILL before the signal handler is
	# installed so we should ensure before returning.
}

sub watch_syslog {
	for (;;) {
		my $tsize = (stat $cfg{syslog})[7];

		# If the syslog file gets rotated, it will magically have a
		# smaller file size.  When we detect this, reload the reader
		# thread.
		reload_syslog() if $tsize < $syslog_filesize;
		sleep(5);
	}
}

sub eject_old {
	lock %fids;
	lock @fids;

	my $amt_reclaimed = 0;
	do {
		my $f = shift @fids;
		delete $fids{$f->{fid}};

		`msctl repl-remove:$cfg{ios}:* $cfg{fsroot}/.slfidns/$f->{fid}`;

	} while ($amt_reclaimed < EJECT_AMT);
}

sub watch_statfs {
	for (;;) {
		my $output = `df \Q$cfg{fsroot}\E`;
		my $pct = $output =~ /(\d+)%/;
		die "unable to parse `df` output: $output\n"
		    unless $pct;
		eject_old() if $pct > $cfg{hi_watermark};
		sleep 5;
	}
}

sub usage {
	die "usage: $0 -f config\n";
}

getopts("f:", \%opts) or usage;
usage() unless $opts{f};

read_conf($opts{f});

scanfs();
threads->create('watch_syslog');
watch_statfs();
