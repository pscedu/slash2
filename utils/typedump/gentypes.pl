#!/usr/bin/perl -W
# $Id$

use Cwd;
use strict;
use warnings;

my $cwd = getcwd;
(my $path = $cwd . "/" . $0) =~ s![^/]*$!!; # !

chdir "$path/../.." or die "chdir $path/../..: $!\n";

my @structs;
my @hdrs = sort <*/*.h>;
foreach my $hdr (@hdrs) {
	open HDR, "<", $hdr;
	while (<HDR>) {
		push @structs, $1 if /^struct (\w+) {/;
	}
	close HDR;
}

open TYPEDUMP, "<", "$path/typedump.c";
my $lines = eval {
	local $/;
	return <TYPEDUMP>;
};
close TYPEDUMP;

my $includes = join '', map { s!include/!!; "#include <$_>\n" } @hdrs;
$lines =~ s!(?<=/\* start includes \*/\n).*?(?=/\* end includes \*/)!$includes!s;

my $structs = join '', map { "PRTYPE(struct $_);\n" } sort @structs;
$lines =~ s!(?<=/\* start structs \*/\n).*?(?=/\* end structs \*/)!$structs!s;

open TYPEDUMP, ">", "$path/typedump.c";
print TYPEDUMP $lines;
close TYPEDUMP;

exit 0;
