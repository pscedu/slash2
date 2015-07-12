#!/usr/bin/perl -W
# $Id$

use Getopt::Std;
use strict;
use threads::shared;
use warnings;
use bignum;

use constant BMAP_SIZE => 128 * 1024 * 1024;

my %opts;
my %fids;
my @ios :shared;
my %ios;
my %cfg;
my $syslog_filesize = 999999999999;
my $syslog_readthr;

sub dprint {
	my ($pkg, $fn, $ln, $sub) = caller(1);
	warn "${0}[$fn:$ln:$sub]: ", @_, "\n" if $cfg{v};
}

sub read_conf {
	my $cfgfn = shift;
	dprint "parsing $cfgfn";
	open CFG or die "open $cfgfn: $!\n";
	while (<CFG>) {
		my ($k, $v) = split /\s+/, $_, 1;
		dprint "setting $k -> $v";
		$cfg{$k} = $v;
	}
	close CFG;
}

sub lru_retail {
	my ($f, $ios) = @_;
	foreach my $ios ($ios ? $ios : @ios) {
		next if $f == $ios->{fid_lru_tail};
		my $idx = $ios->{idx};
		my $next = $f->{next}[$idx];
		my $prev = $f->{prev}[$idx];
		$ios->{fid_lru_head} = $next if $f == $ios->{fid_lru_head};
		$next->{prev}[$idx] = $prev if $next;
		$prev->{next}[$idx] = $next if $prev;
		$f->{next}[$idx] = undef;
		$f->{prev}[$idx] = $ios->{fid_lru_tail};
		$ios->{fid_lru_tail}{next}[$idx] = $f if $ios->{fid_lru_tail};
		$ios->{fid_lru_tail} = $f;
		$ios->{fid_lru_head} = $f unless $ios->{fid_lru_head};
	}
}

sub lru_shift {
	my $ios = shift;
	my $f = $ios->{fid_lru_head};
	$ios->{fid_lru_head} = $f->{next}[$ios->{idx}]
	    if $f == $ios->{fid_lru_head};
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
		if (/ file closed fid=0x0*([a-f0-9]+) /) {
			my $fid = $1;
			lock %fids;

			next unless exists $fids{$fid};
			lru_retail($fids{$fid});

		} elsif (/ repl-add fid=0x0*([a-f0-9]+) ios=(\S+) /) {
			my $fid = $1;
			my $ios_list = $2;
			my @t_ios = split ',', $ios_list;

			lock %fids;

			my $f;
			if (exists $fids{$fid}) {
				$f = $fids{$fid};
			} else {
				$f = $fids{$fid} = newfid $fid;
			}
			foreach my $ios (@t_ios) {
				lru_retail($f, $ios{$ios});
				$f->{refcnt}++;
			}
		}
	}
	close SL;
}

sub newfid {
	my $fid = shift;
	dprint "creating entry for $fid";
	return {
		fid	=> $fid,
		refcnt	=> 0,
		next	=> [ ],
		prev	=> [ ],
	};
}

sub scanfs {
	open P, "msctl -HR repl-status \Q$cfg{fsroot}\E |"
	    or die "msctl: $!\n";
	my $ios;
	my $fn;
	my $f;
	my $fid;
	my %ios_seen;
	while (<P>) {
		if (m[^(/.*)]) {
			my %ios_seen = ();
			my $f = undef;
			$fn = $1;
			$fn =~ s/new-bmap-repl-policy: .*//;
			$fn =~ s/\s+$//;
			$fid = (stat $fn)[1];
		} elsif (/^  (\S+)/) {
			$ios = $1;
		} elsif (/[q+]/ && exists $ios{$ios}) {
			next if $ios_seen{$ios};

			lock %fids;
			if (exists $fids{$fid} && !$f) {
				warn "$fn: fid $fid already seen; " .
				    "hard link perhaps?";
				next;
			}
			$f = $fids{$fid} = newfid $fid unless $f;
			lru_retail($f, $ios{$ios});
			$f->{refcnt}++;
		}
	}
	close P;
}

sub reload_syslog {
	$syslog_readthr->kill('KILL')->detach() if $syslog_readthr;
	dprint "started syslog thread";
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
	my $ios = shift;

	dprint "threshold reached; ejecting for $ios{name}";

	lock %fids;

	my $amt_reclaimed = 0;
	do {
		lock %fids;
		my $f = lru_shift($ios);
		last unless $f;
		delete $fids{$f->{fid}} unless --$f->{refcnt};

		my $fn = "$cfg{fsroot}/.slfidns/$f->{fid}";
		my @out = `msctl -H repl-status $fn`;
		`msctl repl-remove:$cfg{ios}:* $fn`;
		my $fsize = (stat $fn)[7];

		my $ios;
		my $nbmaps = 0;
		shift @out;
		my $last_bmap = 0;
		my $last_line;
		foreach my $ln (@out) {
			if ($ln =~ /^  (\S+)/) {
				$last_bmap = 1 if $ios && $last_line &&
				    $ios eq $ios->{name} &&
				    $last_line =~ /\+$/;
				$ios = $1;
			} elsif (my @m = $ln =~ /\+/g) {
				$nbmaps += @m if $ios eq $ios->{name};
			}
			$last_line = $ln;
		}
		$last_bmap = 1 if $ios && $last_line &&
		    $ios eq $ios->{name} && $last_line =~ /\+$/;

		my $sz = 0;
		$sz += BMAP_SIZE * ($nbmaps - 1) if $nbmaps > 1;
		$sz += $fsize % BMAP_SIZE if $last_bmap;

		$amt_reclaimed += $sz;
		dprint "  + reclaimed $sz (total $amt_reclaimed) " .
		    "for $f->{fid}";

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
		fids		=> [ ],
		fid_lru_head	=> undef,
		fid_lru_tail	=> undef,
	};
	push @ios, $ios;
	$ios{$ios_name} = $ios;
}

scanfs();
threads->create('watch_syslog');
watch_statfs();
