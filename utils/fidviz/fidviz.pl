#!/usr/bin/perl -W
# $Id$

use GD;
use Getopt::Std;
use strict;
use warnings;

use constant WIDTH => 1280;
use constant HEIGHT => 768;

die "usage: $0 > file\n" if -t STDOUT;

my $img = GD::Image->new(WIDTH, HEIGHT);

my $usec = $img->colorAllocate(53, 53, 204);
my $frec = $img->colorAllocate(255, 255, 255);
my $fgc = $img->colorAllocate(0, 10, 31);
my $bgc = $img->colorAllocate(0xff, 0xff, 0xf0);

my $s_cen = sub {
	my ($str, $y, $col) = @_;
	my $fn = gdMediumBoldFont;
	my $len = length($str);
	$img->string($fn, WIDTH / 2 - $fn->width * $len / 2,
	    $y, $str, $col);
};

my $s_errx = sub {
	$s_cen->(join('', @_), 0, $fgc);
	$img->filledRectangle(0, 0, WIDTH, HEIGHT, $bgc);
	$s_cen->("Error: " . join('', @_), 0, $fgc);
	print $img->png();
	exit;
};

my $s_err = sub {
	$s_errx->(@_, ": $!");
};

$img->filledRectangle(0, 0, WIDTH, HEIGHT, $bgc);
$s_cen->("Zest Disk Usage - " . localtime(time()), 0, $fgc);

my $s = IO::Socket::UNIX->new(Peer => $opts{S},
    Type => SOCK_STREAM) or $s_err->("socket $opts{S}");

use constant ZCMT_GETDISK => 13;
use constant ZCTLMSGHDR_SZ => 16;
use constant ZCTLMSG_DISK_SZ => 4464;
my $msg = pack("i2Q", ZCMT_GETDISK, 0, ZCTLMSG_DISK_SZ);
$s->send($msg, 0) or $s_err->($img, "write");

use constant PSCTHR_NAME_MAX => 16;
use constant PCTHRNAME_EVERYONE => "everyone";
use constant PATH_MAX => 4096;
use constant ZDISK_STAT_SZ => 328;

$msg = pack("Z" . PSCTHR_NAME_MAX . "Z" . PATH_MAX . "ia4a" . ZDISK_STAT_SZ,
    PCTHRNAME_EVERYONE, "", 0, "");
$s->send($msg, 0) or $s_err->($img, "write");

$s->shutdown(SHUTDOWN_WR) or $s_err->($img, "shutdown");

my $twidth = WIDTH - 2;
my $siz = ZCTLMSGHDR_SZ;
my ($x, $y) = (0, 30);
my ($buf, $ret);
for (;; $y += 100) {
	defined($s->recv($buf, $siz, 0)) or $s_err->($img, "recv");
	last unless length $buf;

	if (length $buf != $siz) {
		$s_errx->($img, "short recv");
		next;
	}

	my %msg;
	@msg{qw(type id size)} = unpack("i2Q", $buf);

	$s_errx->($img, "received invalid message from zestiond; size=$msg{size}")
	    if $msg{size} < ZCTLMSG_DISK_SZ;
	my $tabsz = $msg{size} - ZCTLMSG_DISK_SZ;
	$s_err->($img, "recv") unless $s->recv($buf, $msg{size}, 0);

	$s_errx->($img, "received invalid message from zestiond")
	    unless $msg{type} == ZCMT_GETDISK;

	%msg = ();
	@msg{qw(thrname dev failed junk stat used)} = unpack(
	    "Z" . ZTHR_NAME_MAX .
	    "Z" . PATH_MAX .
	    "iia" . ZDISK_STAT_SZ .
	    "a" . $tabsz, $buf);

	$s_cen->("thread $msg{thrname}", $y, $fgc);
	$y += 20;

	$img->rectangle($x, $y, WIDTH-1, $y + $tabsz / $twidth + 2, $fgc);
	$img->filledRectangle($x+1, $y+1, WIDTH-3, $y + $tabsz / $twidth + 1, $frec);

	$y++;
	my ($i, $j);
	for ($j = 0, $i = 1; $j < $tabsz; $j++) {
		my $byte = unpack "C", substr($msg{used}, $j / 8, 1);
		$img->setPixel($i, $y, $usec) if $byte & ($j % 8);
		if (++$i >= $twidth) {
			$y++;
			$i = 1;
		}
	}
}

$s->close;

print $img->png();
