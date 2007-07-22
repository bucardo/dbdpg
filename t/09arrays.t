#!perl -w

# Test array stuff - currently not working!

use Test::More qw/no_plan/;
use DBI qw/:sql_types/;
use DBD::Pg qw/:pg_types/;
use strict;
use Data::Dumper;
$|=1;

my ($sth,$result);

if (defined $ENV{DBI_DSN}) {
#	plan tests => 18;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
                       {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for array testing");

if (DBD::Pg::_pg_use_catalog($dbh)) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}
my $pgversion = $dbh->{pg_server_version};

my $SQL = "DELETE FROM dbd_pg_test WHERE pname = 'Array Testing'";
my $cleararray = $dbh->prepare($SQL);

$SQL = "INSERT INTO dbd_pg_test(id,pname,testarray) VALUES (99,'Array Testing',?)";
my $addarray = $dbh->prepare($SQL);

$dbh->{pg_flatten_arrays} = 0;
$SQL = "SELECT testarray FROM dbd_pg_test WHERE pname= 'Array Testing'";
my $getarray = $dbh->prepare($SQL);

my $array_tests = 
q![]
{}
Empty array

[[]]
{{""}}
Empty array with two levels

[[[]]]
{{{""}}}
Empty array with three levels

[[],[]]
{{""},{""}}
Two empty arrays

[[[],[],[]]]
{{{""},{""},{""}}}
Three empty arrays at second level

[[],[[]]]
ERROR: must be of equal size
Unbalanced empty arrays

{}
ERROR: Cannot bind a reference
Bare hashref

[{}]
ERROR: only scalars and other arrays
Hashref at top level

[1,2,{3,4},5]
ERROR: only scalars and other arrays
Hidden hashref

[[1,2],[3]]
ERROR: must be of equal size
Unbalanced array

[[1,2],[3,4,5]]
ERROR: must be of equal size
Unbalanced array

[[1,2],[]]
ERROR: must be of equal size
Unbalanced array

[[],[3]]
ERROR: must be of equal size
Unbalanced array

[123]
{123} quote: {"123"}
Simple 1-D numeric array

['abc']
{abc} quote: {"abc"}
Simple 1-D text array

[1,2]
{1,2} quote: {"1","2"}
Simple 1-D numeric array

[[1]]
{{1}} quote: {{"1"}}
Simple 2-D numeric array

[[1,2]]
{{1,2}} quote: {{"1","2"}}
Simple 2-D numeric array

[[[1]]]
{{{1}}} quote: {{{"1"}}}
Simple 3-D numeric array

[[["alpha",2],[23,"pop"]]]
{{{alpha,2},{23,pop}}} quote: {{{"alpha","2"},{"23","pop"}}}
3-D mixed array

[[[1,2,3],[4,5,"6"],["seven","8","9"]]]
{{{1,2,3},{4,5,6},{seven,8,9}}} quote: {{{"1","2","3"},{"4","5","6"},{"seven","8","9"}}}
3-D mixed array

[q{O'RLY?}]
{O'RLY?} quote: {"O'RLY?"}
Simple single quote

[q{O"RLY?}]
{"O\"RLY?"}
Simple double quote

[[q{O"RLY?}],[q|'Ya' - "really"|],[123]]
{{"O\"RLY?"},{"'Ya' - \"really\""},{123}} quote: {{"O\"RLY?"},{"'Ya' - \"really\""},{"123"}}
Many quotes

["Test\\\\nRun"]
{"Test\\\\nRun"} quote: {"Test\\\\\\nRun"}
Simple backslash

[["Test\\\\nRun","Quite \"so\""],["back\\\\\\\\slashes are a \"pa\\\\in\"",123] ]
{{"Test\\\\nRun","Quite \"so\""},{"back\\\\\\\\\\\\slashes are a \"pa\\\\in\"",123}} quote: {{"Test\\\\\\nRun","Quite \"so\""},{"back\\\\\\\\\\\\slashes are a \"pa\\\\\\in\"","123"}}
Escape party

[undef]
{NULL}
NEED 80200: Simple undef test

[[undef]]
{{NULL}}
NEED 80200: Simple undef test

[[1,2],[undef,3],["four",undef],[undef,undef]]
{{1,2},{NULL,3},{four,NULL},{NULL,NULL}} quote: {{"1","2"},{NULL,"3"},{"four",NULL},{NULL,NULL}}
MEED 80200: Multiple undef test

!;

## Note: We silently allow things like this: [[[]],[]]

for my $test (split /\n\n/ => $array_tests) {
	next unless $test =~ /\w/;
	my ($input,$expected,$msg) = split /\n/ => $test;
	my $qexpected = $expected;
	if ($expected =~ s/\s*quote:\s*(.+)//) {
		$qexpected = $1;
	}

	if ($msg =~ s/NEED (\d+):\s*//) {
		my $ver = $1;
		if ($pgversion < $ver) {
		  SKIP: {
				skip 'Cannot test NULL arrays unless version 8.2 or better', 4;
			}
			next;
		}
	}


	$cleararray->execute();
	eval {
		$addarray->execute(eval $input);
	};
	if ($expected =~ /error:\s+(.+)/i) {
		like($@, qr{$1}, "Array failed : $msg : $input");
	}
	else {
		is($@, q{}, "Array worked : $msg : $input");
		$getarray->execute();
		$result = $getarray->fetchall_arrayref()->[0][0];
		is($result, $expected, "Correct array inserted: $msg : $input");
	}

	eval {
		$result = $dbh->quote(eval $input);
	};
	if ($qexpected =~ /error:\s+(.+)/i) {
		my $errmsg = $1;
		$errmsg =~ s/bind/quote/;
		like($@, qr{$errmsg}, "Array quote failed : $msg : $input");
	}
	else {
		is($@, q{}, "Array quote worked : $msg : $input");
		is($result, $qexpected, "Correct array quote: $msg : $input");
	}

}


$cleararray->execute();
$addarray->execute([123]);
$SQL = "SELECT testarray FROM dbd_pg_test WHERE pname = 'Array Testing'";
$sth = $dbh->prepare($SQL);
$sth->execute();
$result = $sth->fetchall_arrayref()->[0][0];

#is($result, [123], qq{Arrays rock});



## Now the other direction!

ok ($dbh->disconnect, "Disconnect from database");
