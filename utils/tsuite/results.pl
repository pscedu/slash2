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

my $action = $q->param('action') || "";
my $id = $q->param('id') || "";

sub start_page {
	my ($q) = @_;

	print $q->header, <<'EOH';
<!DOCTYPE html>

<html>
	<head>
		<title>SLASH2 tsuite results</title>
		<meta charset="utf-8" />
		<style type='text/css'>
			table {
				background-color: #fdf6e3;
			}
			td, th {
				padding: .2em;
				border: 1px solid #ded8c5;
			}
			td:first-child {
				white-space: nowrap;
			}
			tr:nth-child(even) {
				background: #eee8d5;
			}
			.main a {
				color: #fdf6e3;
			}
			td + td, th + th {
				border-left: 0;
			}
			td {
				border-top: 0;
			}
			th.name {
				background-color: #93a1a1;
				text-align: left;
			}
			th.val {
				background-color: #839496;
				text-align: left;
			}
			th {
				background-color: #268bd2;
				color: #fdf6e3;
				white-space: nowrap;
			}
			.ok {
				background-color: #859900;
				color: #fdf6e3;
			}
			.fail {
				background-color: #dc322f;
				color: #fdf6e3;
			}
			.embed {
				overflow: auto;
				margin: auto;
				margin-bottom: .5em;
			}
			.n {
				text-align: right;
				font-family: monospace;
			}
			.faster {
				background-color: #859900;
				color: #fdf6e3;
			}
			.slower {
				background-color: #dc322f;
				color: #fdf6e3;
			}
			.output {
				font-family: monospace;
				max-width: 900px;
				margin: auto;
				white-space: pre;
				overflow: scroll;
				height: 450px;
			}
			.graph {
				text-align: center;
			}
			.test-name {
				white-space: nowrap;
			}

			h1 {
				text-align: center;
				color: #268bd2;
			}

			h2 {
				color: #cb4b16;
			}

			/* svg objects */
			.axis path, .axis line {
				fill: none;
				stroke: #000;
				shape-rendering: crispEdges;
			}
			.x.axis path {
				display: none;
			}
			.bar {
				fill: orange;
				stroke-width:1;
				stroke:rgb(7,54,66);
			}
			.bar:hover {
				fill: orangered;
			}
			.cur {
				fill: pink;
			}
			.d3-tip {
				line-height: 1;
				font-weight: bold;
				padding: .5em;
				background: rgba(0, 0, 0, 0.8);
				color: #fff;
				border-radius: 2px;
			}

			svg text {
				font-size: 75%;
			}

			/* Creates a small triangle extender for the tooltip */
			.d3-tip:after {
				box-sizing: border-box;
				display: inline;
				font-size: 10px;
				width: 100%;
				line-height: 1;
				color: rgba(0, 0, 0, 0.8);
				content: "\25BC";
				position: absolute;
				text-align: center;
			}

			/* Style northward tooltips differently */
			.d3-tip.n:after {
				margin: -1px 0 0 0;
				top: 100%;
				left: 0;
			}
		</style>
		<script type='text/javascript' src="//d3js.org/d3.v3.min.js"></script>
		<script type='text/javascript' src="//labratrevenge.com/d3-tip/javascripts/d3.tip.v0.6.3.js"></script>
		<script type='text/javascript'>
			function getObj(id) {
				return document.getElementById(id)
			}

			var formatDate = d3.time.format("%y-%m-%d %H");

			function unmarshal(d) {
				// If this seems insane, it's because it is.
				d.date = formatDate.parse(formatDate(new Date(d.date * 1000)))
				// Force type conversion to integer.
				d.duration_s = d.duration_ms / 1000
				return d
			}

			function insertGraph(where, url, runid) {
				w = getObj('main').clientWidth

				var margin = {
					top: 3,
					right: 0,
					bottom: 20,
					left: 50
				}
				var width = w - margin.left - margin.right
				var height = 80

				var x = d3.scale.ordinal()
				    .rangeRoundBands([0, width], .1)

				var y = d3.scale.linear()
				    .range([height, 0])

				var xAxis = d3.svg.axis()
				    .scale(x)
				    .orient("bottom")

				var yAxis = d3.svg.axis()
				    .scale(y)
				    .orient("left")

				var tip = d3.tip()
				    .attr('class', 'd3-tip')
				    .offset([-10, 0])
				    .html(function(d) {
					return "<b>Duration:</b> " + d.duration_s + "(s)"
				    })

				var svg = d3.select(where).append("svg")
				    .attr("width", width + margin.left + margin.right)
				    .attr("height", height + margin.top + margin.bottom)
				    .append("g")
				    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")

				svg.call(tip)

				d3.tsv(url, unmarshal, function(error, data) {
					if (error)
						throw error

					x.domain(data.map(function(d) { return formatDate(d.date) }))
					y.domain([0, d3.max(data, function(d) { return d.duration_s })])

					svg.append("g")
					    .attr("class", "x axis")
					    .attr("transform", "translate(0," + height + ")")
					    .call(xAxis)

					svg.append("g")
					    .attr("class", "y axis")
					    .call(yAxis)

					svg.selectAll(".bar")
					    .data(data)
					  .enter().append("rect")
					    .attr("class", function(d) { return d.run == runid ? "bar cur" : "bar" } )
					    .attr("x", function(d) { return x(formatDate(d.date)) })
					    .attr("width", x.rangeBand())
					    .attr("y", function(d) { return y(d.duration_s) })
					    .attr("height", function(d) { return height - y(d.duration_s) })
					    .on('mouseover', tip.show)
					    .on('mouseout', tip.hide)
				})
			}
			function stringify(o) {
				if (typeof(o) != 'object')
					return o
				var s = ''
				for (var i in o)
					s += i + ':' + stringify(o[i]) + ';\n'
				return s
			}

			window.onload = function(e) {
				var o = getObj('output')
				if (o)
					o.scrollTop = o.scrollHeight
			}
		</script>
	</head>
	<body>
		<h1>SLASH2 tsuite results</h1>
EOH
}

sub end_page {
	print <<EOH;
	</body>
</html>
EOH
}

if ($action eq "getdata") {
	my $test = $q->param('test');
	my $task = $q->param('task');

	my $sth = $dbh->prepare(<<SQL)
		SELECT	STRFTIME('%s', DATETIME(run.launch_date, 'localtime')) AS launch_date,
			res.duration_ms AS duration_ms,
			res.run_id      AS run_id
		FROM	s2ts_result res,
			s2ts_run run
		WHERE	res.test_name = ?
		  AND   res.task_id = ?
		  AND	res.run_id = run.id
		  AND	run.success
SQL
	    or die "unable to execute SQL; err=$DBI::errstr";

	$sth->execute($test, $task) or die "unable to execute SQL; err=$DBI::errstr";

	print $q->header('text/plain'),
		"run\tdate\tduration_ms\n";

	for (my $rows = 0; my $r = $sth->fetchrow_hashref; $rows++) {
		printf "%d\t%d\t%d\n",
		    @{$r}{qw(run_id launch_date duration_ms)};
	}

} elsif ($action eq "view") {

	start_page($q);

	print <<EOH;
		<div style='text-align: center'>
			<a href="?">View all results</a>
		</div>
		<table class='embed' id='main' cellspacing='0' cellpadding='0' border='0'>
EOH

	my $sth = $dbh->prepare(<<SQL)
		SELECT	id,
			suite_name,
			descr,
			DATETIME(launch_date, "localtime") AS launch_date,
			user,
			diff,
			success,
			SUBSTR(output, -10000) AS output,
			sl2_commid,
			pfl_commid
		FROM	s2ts_run
		WHERE	id = ?
SQL
	    or die "unable to execute SQL; err=$DBI::errstr";

	$sth->execute($id) or die "unable to execute SQL; err=$DBI::errstr";

	my $rows = 0;
	my $run = $sth->fetchrow_hashref;

	unless ($run) {
	print <<EOH;
			<tr>
				<th>Error</th>
			</tr>
			<tr>
				<td>Run not found.</td>
			</tr>
EOH
		goto VIEW_OUT;
	}

	print <<EOH;
			<tr>
				<th colspan='2'>$run->{suite_name}</th>
			</tr>
			<tr>
				<td style='width: 15%'>Completion date</td>
				<td>$run->{launch_date}</td>
			</tr>
			<tr>
				<td>User</td>
				<td>$run->{user}</td>
			</tr>
			<tr>
				<td>Status</td>
				<td class='@{[ $run->{success} ?
				 "ok" : "fail" ]}'
				 >@{[ $run->{success} ? "OK" : "FAILED" ]}</td>
			</tr>
			<tr>
				<td>SLASH2 commit</td>
				<td>$run->{sl2_commid}</td>
			</tr>
			<tr>
				<td>PFL commit</td>
				<td>$run->{pfl_commid}</td>
			</tr>
			@{[ $run->{descr} ? <<EOD : "" ]}
			<tr>
				<td colspan='2'>
					$run->{descr}
				</td>
			</tr>
EOD
		</table>

		<table id='result-table' class='embed' cellspacing='0' cellpadding='0' border='0'>
			<tr>
				<th>Test</th>
				<th>Task</th>
				<th style='width: 100%'>Historical Comparison</th>
				<th>Hist Avg</th>
				<th>Duration</th>
				<th>Delta</th>
				<th>%</th>
			</tr>
EOH

	$sth = $dbh->prepare(<<SQL)
		SELECT	*,
			(
				SELECT	AVG(r2.duration_ms)
				FROM	s2ts_result r2,
					s2ts_run run
				WHERE	r2.task_id = res.task_id
				  AND	r2.test_name = res.test_name
				  AND	r2.run_id = run.id
				  AND	run.success
				  AND	run.diff = ''
				  AND	r2.run_id != res.run_id
			) AS havg_ms
		FROM	s2ts_result res
		WHERE	run_id = ?
		ORDER BY
			test_name ASC
SQL
	    or die "unable to execute SQL; err=$DBI::errstr";

	$sth->execute($id) or die "unable to execute SQL; err=$DBI::errstr";

	for (my $rows = 0; my $r = $sth->fetchrow_hashref; $rows++) {
		print <<EOH;
			<tr>
				<td class='test-name'>$r->{test_name}</td>
				<td>$r->{task_id}</td>
				<td class='graph' id='graph$rows'>
EOH
		if ($run->{success}) {
			print <<EOH;
				<script type='text/javascript'>
					insertGraph('#graph$rows', '?action=getdata;test=$r->{test_name};task=$r->{task_id}', $run->{id})
				</script>
EOH
		}

		my $delta = sprintf "%.03f", ($r->{duration_ms} - $r->{havg_ms}) / 1000;
		my $pct = sprintf "%.02f", $delta / $r->{havg_ms} * 100;
		my $cl = "faster";
		unless ($delta =~ /^-/) {
			$delta = "+$delta";
			$pct = "+$pct";
			$cl = "slower";
		}

		print <<EOH;
</td>
				<td class='n'>@{[
				  sprintf "%.03f", $r->{havg_ms} / 1000
				]}</td>
				<td class='n'>@{[
				  sprintf "%.03f", $r->{duration_ms} / 1000
				]}s</td>
				<td class='n $cl'>${delta}s</td>
				<td class='n $cl'>$pct%</td>
			</tr>
EOH
	}

	unless ($run->{success}) {
		print <<EOH;
		</table>

		<table class='embed' cellspacing='0' cellpadding='0' border='0'>
			<tr>
				<th>Output</th>
			</tr>
			<tr>
				<td><div id='output' class='output'
				 >@{[$q->escapeHTML($run->{output})]}</div></td>
			</tr>
EOH
	}

 VIEW_OUT:
	print <<EOH;
	</table>
EOH

	end_page();
} else {
	start_page($q);

	print <<EOH;
		<h2>Latest Results</h2>
		Status: OK

		<h2>Results</h2>

		<table class='embed' id='main' cellspacing='0' cellpadding='0' border='0'>
			<tr>
				<th>Suite</th>
				<th>Launch date</th>
				<th>User</th>
				<th>SLASH2 commit ID</th>
				<th>PFL commit ID</th>
				<th>Status</th>
			</tr>
EOH

	my $sth = $dbh->prepare(<<SQL)
		SELECT	id,
			suite_name,
			descr,
			DATETIME(launch_date, 'localtime') AS launch_date,
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

	for (my $rows = 0; my $r = $sth->fetchrow_hashref; $rows++) {
		print <<EOH;
			<tr>
				<td>$r->{suite_name}</td>
				<td>$r->{launch_date}</td>
				<td>$r->{user}</td>
				<td>$r->{sl2_commid}</td>
				<td>$r->{pfl_commid}</td>
				<td class="@{[ $r->{success} ?
				  "ok" : "fail" ]}"
				  ><a href="?action=view;id=$r->{id}"
				  >@{[ $r->{success} ?
				  "OK" : "FAILED" ]}</a></td>
			</tr>
EOH
	}

	print <<EOH;
	</table>
EOH
	end_page();
}
