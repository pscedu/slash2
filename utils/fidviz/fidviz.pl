#!/usr/bin/perl -W
# $Id$

use strict;
use warnings;

for (my $i = 0; $i < 2**32; $i++) {
	my $hex = sprintf("%lx", $i);
	my $fn = `./fidviz $hex`;
	my @a = split m[/], substr $fn, 15, 7;
	my $n = $a[0] * 16**0+
		$a[1] * 16**1 +
		$a[2] * 16**2 +
		$a[3] * 16**3
	print "$i @a\n";
}
