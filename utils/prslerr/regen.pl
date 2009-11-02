#!/usr/bin/perl -W
# $Id$

use strict;
use warnings;

sub usage {
	die "usage: $0 src hdr\n";
}

sub slurp {
	my $fn = shift;

	local $/;

	open F, "<", $fn or die "$fn: $!\n";
	my $data = <F>;
	close F;

	return ($data);
}

usage unless @ARGV == 2;
my ($src, $hdr) = @ARGV;

my $hdat = slurp $hdr;
my @err = $hdat =~ /\b(SLERR_\w+)/g;

my $ndat = join '', map { qq{\tprintf("%4d [$_]: %s\\n", $_, slstrerror($_));\n} } @err;

my $data = slurp $src;
$data =~ s!(?<=/\* start custom errnos \*/\n).*(?=\t/\* end custom errnos \*/)!$ndat!;

open F, ">", $src or die "$src: $!\n";
print F $data;
close F;

exit 0;
