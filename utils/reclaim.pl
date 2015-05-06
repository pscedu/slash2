#!/usr/bin/perl -W
# $Id: reclaim.pl 24980 2014-12-15 03:58:34Z yanovich $

# Unlink all files on the IOS backing file system that are unknown to
# the SLASH2 MDS.

use Getopt::Std;
use strict;
use warnings;

sub usage {
	die "usage: $0 iosid objfs mountpoint\n";
}

my %opts;
getopts('F', \%opts) or usage;
usage() unless @ARGV == 3;
my ($iosid, $root, $mp) = @ARGV;

my $lastdir = "";
my %fids;		# highest generation # seen for given FID

sub rmfile {
	my $fn = shift;

	print "rm \Q$fn\E\n";
	system("rm \Q$fn\E") if $opts{F};
}

sub basename {
	my $fn = shift;

	$fn =~ s!.*/!!;
	$fn =~ s!/+$!!; # !
	return $fn;
}

die "$root: must specify the .slmd directory\n"
    unless basename($root) eq ".slmd";
chdir($root) or die "$root: $!\n";

open F, "find . -type f |" or die $!;
while (defined(my $fn = <F>)) {

	chomp $fn;
	# parse out S2FID
	unless ($fn =~ m!^(.*)/([0-9a-f]+)_([0-9a-f]+)$!) {
		warn "$fn: unknown garbage\n";
		next;
	}
	my ($dir, $fid, $gen) = ($1, $2, $3);
	$gen = hex($gen);

	my $sl2path = "$mp/.slfidns/$fid";
	my @out;
	{
		delete $ENV{TERM};
		@out = `msctl -Hr \Q$sl2path\E 2>&1`;
	}
	$out[0] = "" unless $out[0];
	map { s/\r//g } @out;

	if ($out[0] =~ m!^msctl: \Q$sl2path\E: No such file or directory$!) {
		rmfile($fn);

	} elsif ($out[0] =~ m!^\Q$sl2path\E\s!) {

		shift @out;
		$out[0] =~ s/^\s+//;
		my @res = split /\n  \b/s, join '', @out;
		my $unlink = 1;
		for my $res (@res) {
			if ($res =~ /^<unknown IOS 0x[a-f0-9]+>\s/) {
			} elsif ($res =~ /^(\w+@\w+)\s/) {
				my $iresid = $1;
				if ($iresid eq $iosid) {
					$unlink = 0 if $' =~ /\+/;
					last;
				}
			} else {
				unless ($res =~ / ^(\w+@\w+)\s/) {
					warn "$fn: parse error ($res)\n";
					next;
				}
			}
		}
		if ($unlink) {
			rmfile($fn);
		} elsif (exists $fids{$fid} and $gen < $fids{$fid}{gid}) {
			rmfile($fn);
		}
		if (!exists $fids{$fid} or $gen > $fids{$fid}{gid}) {
			rmfile($fids{$fid}{fn}) if exists $fids{$fid};
			$fids{$fid} = {
				fn  => $fn,
				gen => $gen,
			};
		}
	} else {
		# genuine error: flag and continue
		warn "$fn: error encountered \Q$sl2path\E:\n";
		warn "output: ",$out[0], "\n";
	}

	# free up memory
	%fids = () unless $lastdir eq $dir;
}
close F;
