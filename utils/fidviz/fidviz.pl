#!/usr/bin/perl -W
# $Id$

use threads;
use strict;
use warnings;

my $dir = "/local/tmp";

sub thrmain {
	my ($i) = @_;
	open F, ">", "$dir/out.$i" or die "$!\n";
	for (; $i < 2**32; $i += 16) {
		my $hex = sprintf("%lx", $i);
		my $fn = `./fidviz $hex`;
		my @a = split m[/], substr $fn, 15, 7;
		my $n = $a[0] * 16**0 +
			$a[1] * 16**1 +
			$a[2] * 16**2 +
			$a[3] * 16**3;
		printf F "%12d %6d\n", $i, $n;
		threads->yield;
	}
	close F;
}

my @thr;
push @thr, threads->create('thrmain', $_) for 0 .. 15;
$_->join for @thr;
