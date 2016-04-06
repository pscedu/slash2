#!/usr/bin/perl -W
# $Id$

use CGI;
use DBI;
use strict;
use warnings;

my $cfg_file = "/home/yanovich/code/advsys/pc6/slash2/utils/tsuite/tests/suite0/cfg";

my $dsn;
open CF, "<", $cfg_file or die "open $cfg_file: $!\n";
while (<CF>) {
	chomp;
	/^\s*#\s*\@dsn\s+/ or next;
	$dsn = $';
}
close CF;

die "no DSN\n" unless $dsn;

my $q = CGI->new;
my $dbh = DBI->connect($dsn);

print $q->header, <<EOH;
<!DOCTYPE html>

<html lang="en-US">
	<head>
		<title>SLASH2 tsuite results</title>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
		<style type='text/css'>
			table {
				background-color: #fdf6e3;
			}
			td, th {
				padding: .2em;
				border: 1px solid #586e75;
			}
			td + td, th + th {
				border-left: 0;
			}
			td {
				border-top: 0;
			}
			th {
				background-color: #9cf;
			}
			.ok {
				background-color: #859900;
				color: #fdf6e3;
			}
			.fail {
				background-color: #dc322f;
				color: #fdf6e3;
			}
		</style>
	</head>
	<body>
		<table cellspacing='0' cellpadding='0' border='0'>
			<tr>
				<th>Suite</th>
				<th>Launch date</th>
				<th>User</th>
				<th>SLASH2 commit ID</th>
				<th>PFL commit ID</th>
				<th>Success</th>
			</tr>
EOH

my $sth = $dbh->prepare(<<SQL)
	SELECT	id,
		suite_name,
		descr,
		launch_date,
		user,
		success,
		sl2_commid,
		pfl_commid
	FROM	s2ts_run
	ORDER BY
		launch_date DESC
SQL
    or die "unable to execute SQL; err=$DBI::errstr";

$sth->execute or die "unable to execute SQL; err=$DBI::errstr";

my $rows = 0;
while (my $r = $sth->fetchrow_hashref) {
	$rows++;
	print <<EOH;
			<tr>
				<td>$r->{suite_name}</td>
				<td>$r->{launch_date}</td>
				<td>$r->{user}</td>
				<td>$r->{sl2_commid}</td>
				<td>$r->{pfl_commid}</td>
				<td class="@{[ $r->{success} ?
				  "ok" : "fail" ]}"
				  ><a href="?">@{[ $r->{success} ?
				  "YES" : "NO" ]}</a></td>
			</tr>
EOH
}

print <<EOH;
		</table>
		$rows row(s)
	</body>
</html>
EOH
