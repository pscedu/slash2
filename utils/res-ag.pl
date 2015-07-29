#!/usr/bin/env perl
# $Id: res-ag.pl 25461 2015-02-28 19:33:38Z yanovich $

# res-ag.pl - Determine aggregate stats of file residencies.
#
# Common usage:
# find /mdsfs -type f -exec dumpfid -o fsize -o repls -o bmaps {} \; | res-ag.pl

use Getopt::Std;
use strict;
use warnings;

sub usage {
	die "usage: $0 [-F spec] [-R id,...] [-S res-state] [file ...]\n";
}

sub in_array {
	my ($needle, $rhay) = @_;
	for my $straw (@$rhay) {
		return 1 if $straw == $needle;
	}
	return 0;
}

my %opts;
getopts('F:R:S:', \%opts) or usage();

$opts{F} = "%n" unless $opts{F};

my @report_excl;
if ($opts{R}) {
	my $r = $opts{R};
	@report_excl = split /\s*,\s*/, $r;
}

@report_excl = map { /^0x/ ? hex($_) : $_ } @report_excl;

{
	my %tab = (
		f => "fid",
		n => "fn",
		u => "usr",
		g => "grp",
	);

	sub dispf {
		my ($rf) = @_;
		my $str = $opts{F};
		$str =~ s/%([a-z])/$rf->{$tab{$1}}/g;
		print "$str\n";
	}
}

my $nr = 0; # total number of residencies of all bmaps
my $tb = 0; # total unique bmaps
my $fn;
my %f;
my %frepl; # file repls
my @rids;
#my %grepl; # aggregate repl stats
my $has_other_res = 1;
my @nxmap;
while (<>) {
	chomp;
	if (m!^\S!) {
		dispf(\%f) unless $has_other_res;

		($fn = $_) =~ s/:$//;
		%f = (fn => $fn);
		$has_other_res = 1;
#		%frepl = ();
	} elsif (/^\s+repls\s+(.*)/) {
		my $r = $1;
		@rids = split /,/, $r;
		@nxmap = ();
		for (my $i = 0; $i < @rids; $i++) {
			$nxmap[$i] = !in_array($rids[$i],
			    \@report_excl);
		}
#		@frepls{@rids} = (0) x @rids;
	} elsif (/^\s+(\d+):.*res (.*)/) {

		my $bn = $1;
		my $map = $2;

		$tb++;
		my $found = 0;

		for (my $i = 0; $i < length $map; $i++) {
			my $c = substr $map, $i, 1;
			if ($c eq "+") {
				$nr++;
				$found = 1 if $nxmap[$i];
			} elsif (exists $opts{S} && $c eq $opts{S}) {
				print "$rids[$i]:$fn:$bn\n";
			}
		}

		$has_other_res = 0 unless $found;
	} elsif (/^\s+(usr|grp|fid) (.*)/) {
		$f{$1} = $2;
	}
}

if ($fn) {
	print "$fn\n" unless $has_other_res;
}

print "bmaps: $tb\n";
print "residencies: $nr\n";
print "multiresidency: ", $nr/$tb, "\n";
