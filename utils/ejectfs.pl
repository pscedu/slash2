#!/usr/bin/perl -W
# $Id$

use Getopt::Std;
use strict;
use threads::shared;
use warnings;
use bignum;

my %opts;
my @ios :shared;
my %cfg;
my $syslog_filesize		= 999999999999;
my $syslog_readthr;

sub dprint {
	warn @_, "\n" if $cfg{v};
}

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
	my ($f, $ios) = shift;
	return if $f == $ios->{fid_lru_tail};
	my $next = $f->{next}[];
	my $prev = $f->{prev};
	$ios->{fid_lru_head} = $next if $f == $ios->{fid_lru_head};
	$next->{prev} = $prev if $next;
	$prev->{next} = $next if $prev;
	$f->{next} = undef;
	$f->{prev} = $ios->{fid_lru_tail};
	$ios->{fid_lru_tail}{next} = $f if $ios->{fid_lru_tail};
	$ios->{fid_lru_tail} = $f;
	$ios->{fid_lru_head} = $f unless $ios->{fid_lru_head};
}

sub lru_shift {
	my $ios = shift;
	my $f = $ios->{fid_lru_head};
	$ios->{fid_lru_head} = $f->{next} if $f;
	return $f;
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
			my $f = $fids{$fid} = newfid $fid;
			lru_retail($f);
		}
	}
	close SL;
}

sub newfid {
	my $fid = shift;
	return {
		fid	=> $fid,
		next	=> [ ],
		prev	=> [ ],
	};
}

sub scanfs {
	open P, "msctl -HR repl-status \Q$cfg{fsroot}\E |"
	    or die "msctl: $!\n";
	while (<P>) {
		if (m[^(/.*)]) {
			my $fn = $1;
			$fn =~ s/new-bmap-repl-policy: .*//;
			$fn =~ s/\s+$//;
			my $fid = (stat $fn)[1];
		} elsif (/^  (\S+)/) {
			my $ios = $1;
		} else {
		}
	}
	close P;
}

sub reload_syslog {
	$syslog_readthr->kill('KILL')->detach() if $syslog_readthr;
	$syslog_readthr = threads->create('read_syslog');

	# XXX There is a race if we send a KILL before the signal
	# handler is installed so we should ensure before returning.
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

	my $amt_reclaimed = 0;
	do {
		lock %fids;
		my $f = lru_shift;
		delete $fids{$f->{fid}};

		my $fn = "$cfg{fsroot}/.slfidns/$f->{fid}";
		my @out = `msctl -H repl-status $fn`;
		`msctl repl-remove:$cfg{ios}:* $fn`;

		my $fsize = (stat $fn)[7];

		my $ios;
		my $nfound = 0;
		shift @out;
		foreach my $ln (@out) {
			if ($ln =~ /^  (\S+)/) {
				$ios = $1;
			} elsif (my @m = $ln =~ /\+/g) {
			}
		}
		$amt_reclaimed += $;

	} while ($amt_reclaimed < $cfg{eject_amt});
}

sub watch_statfs {
	for (;;) {
		foreach my $ios (@ios) {
			system("msctl -p sys.pref_ios=$ios");
			my $output = `df \Q$cfg{fsroot}\E`;
			my $pct = $output =~ /(\d+)%/;
			die "unable to parse `df` output: $output\n"
			    unless $pct;
			eject_old($ios) if $pct > $cfg{hi_watermark};
		}
		sleep 5;
	}
}

sub usage {
	die "usage: $0 [-v] -f config\n";
}

getopts("f:v", \%opts) or usage;
usage() unless $opts{f};

read_conf($opts{f});
my $idx = 0;
foreach my $ios_name (split /[, \t]+/, $cfg{ios}) {
	my $ios = {
		idx		=> $idx++,
		name		=> $ios_name,
		fids		=> { },
		fid_lru_head	=> undef,
		fid_lru_tail	=> undef,
	};
	push @ios, $ios;
}

scanfs();
threads->create('watch_syslog');
watch_statfs();
