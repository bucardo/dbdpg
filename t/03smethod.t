#!perl -w

# Test of the statement handle methods
# The following methods are *not* currently tested here:
# "bind_param_inout"
# "execute"
# "finish"
# "dump_results"

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 48;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for statement handle method testing");

$dbh->do("DELETE FROM dbd_pg_test");
my ($SQL, $sth, $result, @result, $expected, $warning, $rows);

#
# Test of the "bind_param" statement handle method
#

$SQL = "SELECT id FROM dbd_pg_test WHERE id = ?";
$sth = $dbh->prepare($SQL);
ok( $sth->bind_param(1, 1), 'Statement handle method "bind_param" works when binding an int column with an int');
ok( $sth->bind_param(1, 'foo'), 'Statement handle method "bind_param" works when rebinding an int column with a string');

# Check if the server is sending us warning messages
# We assume that older servers are okay
my $pgversion = DBD::Pg::_pg_server_version($dbh);
my $client_level = '';
if (DBD::Pg::_pg_check_version(7.3, $pgversion)) {
	$sth = $dbh->prepare("SHOW client_min_messages");
	$sth->execute();
	$client_level = $sth->fetchall_arrayref()->[0][0];
}

# Make sure that we get warnings when we try to use SQL_BINARY.
if ($client_level eq "error") {
 SKIP: {
		skip "Cannot check warning on SQL_BINARY because client_min_messages is set to 'error'", 2;
	}
}
else {
	$dbh->{Warn} = 1;
	my $warning;
	{
		local $SIG{__WARN__} = sub { $warning = "@_" };
		$SQL = "SELECT id FROM dbd_pg_test WHERE id = ?";
		$sth = $dbh->prepare($SQL);
		$sth->bind_param(1, 'foo', DBI::SQL_BINARY);
		like( $warning, qr/^Use of SQL_BINARY/, 'Statement handle method "bind_param" given a warning when binding SQL_BINARY');
	}
}

#
# Test of the "bind_param_array" statement handle method
#

$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, val) VALUES (?,?)');
# Try with 1, 2, and 3 values. All should succeed
eval {
	$sth->bind_param_array(1, [ 30, 31, 32 ]);
};
ok( !$@, 'Statement handle method "bind_param_array" works binding three values to the first placeholder');

eval {
	$sth->bind_param_array(2, 'Mulberry');
};
ok( !$@, 'Statement handle method "bind_param_array" works binding one scalar value to the second placeholder');

eval {
	$sth->bind_param_array(2, [ 'Mango', 'Strawberry', 'Gooseberry' ]);
};
ok( !$@, 'Statement handle method "bind_param_array" works binding three values to the second placeholder');

eval {
	$sth->bind_param_array(1, [ 30 ]);
};
ok( $@, 'Statement handle method "bind_param_array" fails when binding one value to the first placeholder');

eval {
	$sth->bind_param_array(2, [ 'Plantain', 'Apple' ]);
};
ok( $@, 'Statement handle method "bind_param_array" fails when binding two values to the second placeholder');

#
# Test of the "execute_array" statement handle method
#

$dbh->{RaiseError}=0;
my @tuple_status;
$rows = $sth->execute_array( { ArrayTupleStatus => \@tuple_status });
is_deeply( \@tuple_status, [1,1,1], 'Statement method handle "execute_array" works');
is( $rows, 3, 'Statement method handle "execute_array" returns correct number of rows');

# Test the ArrayTupleFetch attribute
$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, val) VALUES (?,?)');
# Try with 1, 2, and 3 values. All should succeed
$sth->bind_param_array(1, [ 20, 21, 22 ]);
$sth->bind_param_array(2, 'fruit');

my $counter=0;
my @insertvals = (
									[33 => 'Peach'],
									[34 => 'Huckleberry'],
									[35 => 'Guava'],
									[36 => 'Lemon'],
								 );
sub getval {
	return $insertvals[$counter++];
}

undef @tuple_status;
$rows = $sth->execute_array( { ArrayTupleStatus => \@tuple_status, ArrayTupleFetch => \&getval });
is_deeply( \@tuple_status, [1,1,1,1], 'Statement method handle "execute_array" works with ArrayTupleFetch');

is( $rows, 4, 'Statement method handle "execute_array" returns correct number of rows with ArrayTupleFetch');

#
# Test of the "execute_for_fetch" statement handle method
#

$sth = $dbh->prepare("SELECT id+200, val FROM dbd_pg_test");
my $goodrows = $sth->execute();
my $sth2 = $dbh->prepare("INSERT INTO dbd_pg_test (id, val) VALUES (?,?)");
$sth2->execute();
my $fetch_tuple_sub = sub { $sth->fetchrow_arrayref() };
undef @tuple_status;
$rows = $sth2->execute_for_fetch($fetch_tuple_sub, \@tuple_status);
is_deeply( \@tuple_status, [map{1}(1..$goodrows)], 'Statement handle method "execute_for_fetch" works');
is( $rows, $goodrows, 'Statement handle method "execute_for_fetch" returns correct number of rows');

#
# Test of the "fetchrow_arrayref" statement handle method
#

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id = 34");
$sth->execute();
$result = $sth->fetchrow_arrayref();
is_deeply( $result, [34, 'Huckleberry'], 'Statement handle method "fetchrow_arrayref" returns first row correctly');
$result = $sth->fetchrow_arrayref();
is_deeply( $result, undef, 'Statement handle method "fetchrow_arrayref" returns undef when done');

# Test of the "fetch" alias
$sth->execute();
$result = $sth->fetch();
$expected = [34, 'Huckleberry'];
is_deeply( $result, $expected, 'Statement handle method alias "fetch" returns first row correctly');
$result = $sth->fetch();
is_deeply( $result, undef, 'Statement handle method alias "fetch" returns undef when done');

#
# Test of the "fetchrow_array" statement handle method
#

$sth->execute();
@result = $sth->fetchrow_array();
is_deeply( \@result, $expected, 'Statement handle method "fetchrow_array" returns first row correctly');
@result = $sth->fetchrow_array();
is_deeply( \@result, [], 'Statement handle method "fetchrow_array" returns an empty list when done');

#
# Test of the "fetchrow_hashref" statement handle method
#

$sth->execute();
$result = $sth->fetchrow_hashref();
$expected = {id => 34, val => 'Huckleberry'};
is_deeply( $result, $expected, 'Statement handle method "fetchrow_hashref" works with a slice argument');
$result = $sth->fetchrow_hashref();
is_deeply( $result, undef, 'Statement handle method "fetchrow_hashref" returns undef when done');

#
# Test of the "fetchall_arrayref" statement handle method
#

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id IN (35,36) ORDER BY id ASC");
$sth->execute();
$result = $sth->fetchall_arrayref();
$expected = [[35,'Guava'],[36,'Lemon']];
is_deeply( $result, $expected, 'Statement handle method "fetchall_arrayref" returns first row correctly');
$result = $sth->fetchall_arrayref();
is_deeply( $result, [], 'Statement handle method "fetchall_arrayref" returns an empty list when done');

# Test of the 'slice' argument

$sth->execute();
$result = $sth->fetchall_arrayref([1]);
$expected = [['Guava'],['Lemon']];
is_deeply( $result, $expected, 'Statement handle method "fetchall_arrayref" works with an arrayref slice');

$sth->execute();
$result = $sth->fetchall_arrayref({id => 1});
$expected = [{id => 35},{id => 36}];
is_deeply( $result, $expected, 'Statement handle method "fetchall_arrayref" works with a hashref slice');

# My personal favorite way of grabbing data
$sth->execute();
$result = $sth->fetchall_arrayref({});
$expected = [{id => 35, val => 'Guava'},{id => 36, val => 'Lemon'}];
is_deeply( $result, $expected, 'Statement handle method "fetchall_arrayref" works with an empty hashref slice');

# Test of the 'maxrows' argument
$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id >= 33 ORDER BY id ASC LIMIT 10");
$sth->execute();
$result = $sth->fetchall_arrayref(undef,2);
$expected = [[33,'Peach'],[34,'Huckleberry']];
is_deeply( $result, $expected, qq{Statement handle method "fetchall_arrayref" works with a 'maxrows' argument});
$result = $sth->fetchall_arrayref([1],2);
$expected = [['Guava'],['Lemon']];
is_deeply( $result, $expected, qq{Statement handle method "fetchall_arrayref" works with an arrayref slice and a 'maxrows' argument});
$sth->finish();

#
# Test of the "fetchall_hashref" statement handle method
#

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)");
$sth->execute();
eval {
	$sth->fetchall_hashref();
};
ok( $@, 'Statement handle method "fetchall_hashref" gives an error when called with no arguments');

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)");
$sth->execute();
$result =	$sth->fetchall_hashref('id');
$expected = {33=>{id => 33, val => 'Peach'},34=>{id => 34, val => 'Huckleberry'}};
is_deeply( $result, $expected, qq{Statement handle method "fetchall_hashref" works with a named key field});
$sth->execute();
$result =	$sth->fetchall_hashref(1);
is_deeply( $result, $expected, qq{Statement handle method "fetchall_hashref" works with a numeric key field});
$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id < 1");
$sth->execute();
$result =	$sth->fetchall_hashref(1);
is_deeply( $result, {}, qq{Statement handle method "fetchall_hashref" returns an empty hash when no rows returned});

#
# Test of the "rows" statement handle method
#

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)");
$rows = $sth->rows();
is( $rows, 0, 'Statement handle method "rows" returns 0 before an execute');
$sth->execute();
$rows = $sth->rows();
is( $rows, 2, 'Statement handle method "rows" returns correct number of rows');
$sth->finish();

#
# Test of the "bind_col" statement handle method
#

$sth = $dbh->prepare("SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)");
$sth->execute();
my $bindme;
$result = $sth->bind_col(2, \$bindme);
is( $result, 1, 'Statement handle method "bind_col" returns the correct value');
$sth->fetch();
is( $bindme, 'Peach', 'Statement handle method "bind_col" correctly binds parameters');

#
# Test of the "bind_columns" statement handle method
#

$sth->execute();
my $bindme2;
eval {
	$sth->bind_columns(1);
};
ok( $@, 'Statement handle method "bind_columns" fails when called with wrong number of arguments');
$result = $sth->bind_columns(\$bindme, \$bindme2);
is($result, 1, 'Statement handle method "bind_columns" returns the correct value');
$sth->fetch();
$expected = [33, 'Peach'];
is_deeply( [$bindme, $bindme2], $expected, 'Statement handle method "bind_columns" correctly binds parameters');
$sth->finish();

#
# Test of the "pg_size" statement handle method
#

$SQL = 'SELECT id, pname, val, score, Fixed, pdate, "CaseTest" FROM dbd_pg_test';
$sth = $dbh->prepare($SQL);
$sth->execute();
$result = $sth->{pg_size};
$expected = [qw(4 -1 -1 8 -1 8 1)];
is_deeply( $result, $expected, 'Statement handle method "pg_size" works');

#
# Test of the "pg_type" statement handle method
#

$sth->execute();
$result = $sth->{pg_type};
$expected = [qw(int4 varchar text float8 bpchar timestamp bool)];
# Hack for old servers
$expected->[5] = 'datetime' if (! DBD::Pg::_pg_check_version(7.3, $pgversion));
is_deeply( $result, $expected, 'Statement handle method "pg_type" works');
$sth->finish();

#
# Test of the "pg_oid_status" statement handle method
#

$SQL = "INSERT INTO dbd_pg_test (id, val) VALUES (?, 'lemon')";
$sth = $dbh->prepare($SQL);
$sth->execute(500);
$result = $sth->{pg_oid_status};
like( $result, qr/^\d+$/, 'Statement handle method "pg_oid_status" returned a numeric value after insert');

#
# Test of the "pg_cmd_status" statement handle method
#

## INSERT DELETE UPDATE SELECT
for ("INSERT INTO dbd_pg_test (id,val) VALUES (400, 'lime')",
		 "DELETE FROM dbd_pg_test WHERE id=1",
		 "UPDATE dbd_pg_test SET id=2 WHERE id=2",
		 "SELECT * FROM dbd_pg_test"
		) {
	my $expected = substr($_,0,6);
	$sth = $dbh->prepare($_);
	$sth->execute();
	$result = $sth->{pg_cmd_status};
	$sth->finish();
	like ( $result, qr/^$expected/, qq{Statement handle method "pg_cmd_status" works for '$expected'});
}

$dbh->disconnect();



