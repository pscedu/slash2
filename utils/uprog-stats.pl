#!/usr/bin/perl -W
# $Id: uprog-stats.pl 25252 2015-01-21 04:43:07Z yanovich $

# This program was written to aggregate usage stats from SLASH2 logs
# in a breakdown of top applications.

# example usage:
# { cd /var/log/arc/ ; bzcat mount_slash-* ; cat mount_slash ; } | \
#    perl uprog-stats.pl

use strict;
use warnings;

my %bytes_rd;
my %bytes_wr;
my %fsize;
my %users;
my %nfiles;

sub update_stat {
	my ($t, $uprog, $st) = @_;

	if (exists $t->{$uprog}) {
		$t->{$uprog} += $st;
	} else {
		$t->{$uprog} = $st;
	}
}

my %p;
while (<>) {
	my ($uid, $fsize, $rd, $wr, $uprog) =
	    /file \s closed \s .* \s
		uid=(\d+) \s .* \s
		fsize=(\d+) \s .* \s
		rd=(\d+) \s wr=(\d+) \s
		prog=(.*)/x or next;
	$uprog =~ s!^\S*/!!;
	$uprog =~ s/^ksh93|^bash\s+\-//;
	$uprog = "?" unless $uprog;
	update_stat \%bytes_rd, $uprog, $rd;
	update_stat \%bytes_wr, $uprog, $wr;
	update_stat \%fsize, $uprog, $fsize;
	update_stat \%nfiles, $uprog, 1;
	if (exists $users{$uprog}) {
		if (exists $users{$uprog}{$uid}) {
			$users{$uprog}{$uid}++;
		} else {
			$users{$uprog}{$uid} = 1;
		}
	} else {
		$users{$uprog} = {
			$uid => 1,
		};
	}
}

use constant F_HUMANSIZE => (1 << 0);

sub humansize {
	my $n = shift;
	my @suf = ("", qw(K M G T));

	while ($n > 1024) {
		shift @suf;
		$n /= 1024;
	}
	return sprintf "%.2f%s", $n, $suf[0];
}

sub print_stats {
	my ($h, $label, $fl) = @_;

	my @output;
	my @k = sort {
		$h->{$b} <=> $h->{$a}
	} keys %$h;
	print "<div style='float:left; margin-right: .5em'>\n";
	print "|  *Program*  |$label|\n";
	for (my $i = 0; $i < 10; $i++) {
		my $uprog = $k[$i];
		print "|$uprog|";
		if ($fl & F_HUMANSIZE) {
			print "  " . humansize($h->{$uprog});
		} else {
			print "  " . $h->{$uprog};
		}
		print "|\n";
	}
	print "</div>\n";
}

print_stats(\%nfiles, "  *#Files*  ", 0);
#print_stats(\%fsize, "  *File Size*  ", F_HUMANSIZE);
print_stats(\%bytes_rd, "  *Read*  ", F_HUMANSIZE);
print_stats(\%bytes_wr, "  *Written*  ", F_HUMANSIZE);
print "<br style='clear:both' />\n";

