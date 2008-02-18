#!perl

## Test of the statement handle methods
## The following methods are *not* currently tested here:
## "execute"
## "finish"
## "dump_results"

use strict;
use warnings;
use Test::More;
use DBI ':sql_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (defined $dbh) {
	plan tests => 75;
}
else {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

ok( defined $dbh, 'Connect to database for statement handle method testing');

my $pglibversion = $dbh->{pg_lib_version};

my ($SQL, $sth, $sth2, $result, @result, $expected, $warning, $rows, $t);

#
# Test of the prepare flags
#

$t=q{Calling prepare() with no arguments gives an error};
eval{ $sth = $dbh->prepare(); };
like($@, qr{\+ 0}, $t);

$t=q{Calling prepare() with an undefined value returns undef};
$sth = $dbh->prepare(undef);
is($sth, undef, $t);

$SQL = 'SELECT id FROM dbd_pg_test WHERE id = ?';
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with no flags works');
$dbh->{pg_server_prepare} = 0;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_server_prepare off at database handle works');
## 7.4 does not have a full SSP implementation, so we simply skip these tests.
if ($pglibversion < 80000) {
 SKIP: {
		skip 'Not testing pg_server_prepare on 7.4-compiled servers', 2;
	}
}
else {
	$dbh->{pg_server_prepare} = 1;
	$sth = $dbh->prepare($SQL);
	$sth->execute(1);
	ok( $sth->execute, 'Prepare/execute with pg_server_prepare on at database handle works');
}

## We must send a hashref as the final arg
eval {
	$sth = $dbh->prepare('SELECT 123', ['I am not a hashref!']);
};
like ($@, qr{not a hash}, 'Prepare failes when sent a non-hashref');


# Make sure that undefs are converted to NULL.
$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, pdate) VALUES (?,?)');
ok( $sth->execute(401, undef), 'Prepare/execute with undef converted to NULL');
$sth = $dbh->prepare($SQL, {pg_server_prepare => 0});
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_server_prepare off at statement handle works');
if ($pglibversion >= 80000) {
	$sth = $dbh->prepare($SQL, {pg_server_prepare => 1});
	$sth->execute(1);
	ok( $sth->execute, 'Prepare/execute with pg_server_prepare on at statement handle works');
}
$dbh->{pg_prepare_now} = 1;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_prepare_now on at database handle works');
$dbh->{pg_prepare_now} = 0;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_prepare_now off at database handle works');
$sth = $dbh->prepare($SQL, {pg_prepare_now => 0});
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_prepare_now off at statement handle works');
$sth = $dbh->prepare($SQL, {pg_prepare_now => 1});
$sth->execute(1);
ok( $sth->execute, 'Prepare/execute with pg_prepare_now on at statement handle works');

# Test using our own prepared statements
my $pgversion = $dbh->{pg_server_version};
my $myname = 'dbdpg_test_1';
$dbh->do("PREPARE $myname(int) AS SELECT COUNT(*) FROM pg_class WHERE reltuples > \$1", {pg_direct=> 1});
$sth = $dbh->prepare('SELECT ?');
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->{pg_prepare_name} = $myname;
ok($sth->execute(1), 'Prepare/execute works with pg_prepare_name');
$dbh->do("DEALLOCATE $myname");


#
# Test of the "bind_param" statement handle method
#

$SQL = 'SELECT id FROM dbd_pg_test WHERE id = ?';
$sth = $dbh->prepare($SQL);
ok( $sth->bind_param(1, 1), 'Statement handle method "bind_param" works when binding an int column with an int');
ok( $sth->bind_param(1, 'foo'), 'Statement handle method "bind_param" works when rebinding an int column with a string');

# Check if the server is sending us warning messages
# We assume that older servers are okay
my $client_level = '';
$sth2 = $dbh->prepare('SHOW client_min_messages');
$sth2->execute();
$client_level = $sth2->fetchall_arrayref()->[0][0];

#
# Test of the "bind_param_inout" statement handle method
#

$t = q{Values do not change if bind_param_inout is not called};

my $var = 123;
$sth = $dbh->prepare('SELECT 1+?::int');

$t = q{Invalid placeholder fails for bind_param_inout};
eval { $sth->bind_param_inout(0, \$var, 0); };
like($@, qr{Cannot bind}, $t);

eval { $sth->bind_param_inout(3, \$var, 0); };
like($@, qr{Cannot bind}, $t);

$t = q{Calling bind_param_inout with a non-scalar reference fails};
eval { $sth->bind_param_inout(1, 'noway', 0); };
like($@, qr{needs a reference}, $t);

eval { $sth->bind_param_inout(1, $t, 0); };
like($@, qr{needs a reference}, $t);

eval { $sth->bind_param_inout(1, [123], 0); };
like($@, qr{needs a reference}, $t);


$t = q{Calling bind_param_inout changes an integer value};
eval { $sth->bind_param_inout(1, \$var, 0); };
is($@, q{}, $t);
$var = 999;
$sth->execute();
$sth->fetch;
is($var, 1000, $t);

$t = q{Calling bind_param_inout changes a string value};
$sth = $dbh->prepare(q{SELECT 'X'||?::text});
$sth->bind_param_inout(1, \$var, 0);
$var = 'abc';
$sth->execute();
$sth->fetch;
is($var, 'Xabc', $t);

$t = q{Calling bind_param_inout changes a string to a float};
$sth = $dbh->prepare('SELECT ?::float');
$sth->bind_param_inout(1, \$var, 0);
$var = '1e+6';
$sth->execute();
$sth->fetch;
is($var, '1000000', $t);

$t = q{Calling bind_param_inout works for second placeholder};
$sth = $dbh->prepare('SELECT ?::float, 1+?::int');
$sth->bind_param_inout(2, \$var, 0);
$var = 111;
$sth->execute(222,333);
$sth->fetch;
is($var, 112, $t);

$t = q{Calling bind_param_inout changes two variables at once};
my $var2 = 234;
$sth = $dbh->prepare('SELECT 1+?::float, 1+?::int');
$sth->bind_param_inout(1, \$var, 0);
$sth->bind_param_inout(2, \$var2, 0);
$var = 444; $var2 = 555;
$sth->execute();
$sth->fetch;
is($var, 445, $t);
is($var2, 556, $t);

#
# Test of the "bind_param_array" statement handle method
#

$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, val) VALUES (?,?)');
# Try with 1, 2, and 3 values. All should succeed

eval {
	$sth->bind_param_array(1, [ 30, 31, 32 ], SQL_INTEGER);
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

$dbh->{RaiseError}=1;
my @tuple_status;
$rows = $sth->execute_array( { ArrayTupleStatus => \@tuple_status });
is_deeply( \@tuple_status, [1,1,1], 'Statement method handle "execute_array" works');
is( $rows, 3, 'Statement method handle "execute_array" returns correct number of rows');

# Test the ArrayTupleFetch attribute
$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, val) VALUES (?,?)');
# Try with 1, 2, and 3 values. All should succeed
$sth->bind_param_array(1, [ 20, 21, 22 ], SQL_INTEGER);
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

$sth = $dbh->prepare('SELECT id+200, val FROM dbd_pg_test');
my $goodrows = $sth->execute();
$sth2 = $dbh->prepare(q{INSERT INTO dbd_pg_test (id, val) VALUES (?,?)});
$sth2->bind_param(1,'',SQL_INTEGER);
my $fetch_tuple_sub = sub { $sth->fetchrow_arrayref() };
undef @tuple_status;
$rows = $sth2->execute_for_fetch($fetch_tuple_sub, \@tuple_status);

is_deeply( \@tuple_status, [map{1}(1..$goodrows)], 'Statement handle method "execute_for_fetch" works');

is( $rows, $goodrows, 'Statement handle method "execute_for_fetch" returns correct number of rows');

#
# Test of the "fetchrow_arrayref" statement handle method
#

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id = 34');
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

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (35,36) ORDER BY id ASC');
$sth->execute();
$result = $sth->fetchall_arrayref();
$expected = [[35,'Guava'],[36,'Lemon']];
is_deeply( $result, $expected, 'Statement handle method "fetchall_arrayref" returns first row correctly');

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
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id >= 33 ORDER BY id ASC LIMIT 10');
$sth->execute();
$result = $sth->fetchall_arrayref(undef,2);
$expected = [[33,'Peach'],[34,'Huckleberry']];
is_deeply( $result, $expected, q{Statement handle method "fetchall_arrayref" works with a 'maxrows' argument});
$result = $sth->fetchall_arrayref([1],2);
$expected = [['Guava'],['Lemon']];
is_deeply( $result, $expected, q{Statement handle method "fetchall_arrayref" works with an arrayref slice and a 'maxrows' argument});
$sth->finish();

#
# Test of the "fetchall_hashref" statement handle method
#

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
eval {
	$sth->fetchall_hashref();
};
ok( $@, 'Statement handle method "fetchall_hashref" gives an error when called with no arguments');

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
$result = $sth->fetchall_hashref('id');
$expected = {33=>{id => 33, val => 'Peach'},34=>{id => 34, val => 'Huckleberry'}};
is_deeply( $result, $expected, q{Statement handle method "fetchall_hashref" works with a named key field});
$sth->execute();
$result = $sth->fetchall_hashref(1);
is_deeply( $result, $expected, q{Statement handle method "fetchall_hashref" works with a numeric key field});
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id < 1');
$sth->execute();
$result = $sth->fetchall_hashref(1);
is_deeply( $result, {}, q{Statement handle method "fetchall_hashref" returns an empty hash when no rows returned});

#
# Test of the "rows" statement handle method
#

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$rows = $sth->rows();
is( $rows, -1, 'Statement handle method "rows" returns -1 before an execute');
$sth->execute();
$rows = $sth->rows();
is( $rows, 2, 'Statement handle method "rows" returns correct number of rows');
$sth->finish();

#
# Test of the "bind_col" statement handle method
#

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
my $bindme;
$result = $sth->bind_col(2, \$bindme);
is( $result, 1, 'Statement handle method "bind_col" returns the correct value');
$sth->fetch();
is( $bindme, 'Peach', 'Statement handle method "bind_col" correctly binds parameters');

$dbh->do(q{UPDATE dbd_pg_test SET testarray = '{2,3,55}' WHERE id = 33});

$t=q{Statement handle method 'bind_col' works with arrays};
my $bindarray;
$sth = $dbh->prepare('SELECT id, testarray FROM dbd_pg_test WHERE id = 33');
$sth->execute();
$result = $sth->bind_col(1, \$bindme);
is( $result, 1, 'Statement handle method "bind_col" returns the correct value');
$result = $sth->bind_col(2, \$bindarray);
is( $result, 1, 'Statement handle method "bind_col" returns the correct value');
$sth->fetch();
is( $bindme, '33', 'Statement handle method "bind_col" correctly binds parameters');
is_deeply( $bindarray, [2,3,55], 'Statement handle method "bind_col" correctly binds arrayref');


#
# Test of the "bind_columns" statement handle method
#

$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34) ORDER BY id');
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
my $got = [$bindme, $bindme2];
is_deeply( $got, $expected, 'Statement handle method "bind_columns" correctly binds parameters');
$sth->finish();

#
# Test of the statement handle method "state"
#

$result = $sth->state();
is( $result, q{}, q{Statement handle method "state" returns an empty string on success});

eval {
	$sth = $dbh->prepare('SELECT dbdpg_throws_an_error');
	$sth->execute();
};
$result = $sth->state();
like( $result, qr/^[A-Z0-9]{5}$/, q{Statement handle method "state" returns a five-character code on error});
my $result2 = $dbh->state();
is ($result, $result2, q{Statement and database handle method "state" return same code});
is ($result, '42703', q{Statement handle method "state" returns expected code});

#
# Test of the statement handle method "private_attribute_info"
#

SKIP: {
	if ($DBI::VERSION < 1.54) {
		skip 'DBI must be at least version 1.54 to test private_attribute_info', 2;
	}

	$sth = $dbh->prepare('SELECT 123');
	my $private = $sth->private_attribute_info();
	my ($valid,$invalid) = (0,0);
	for my $name (keys %$private) {
		$name =~ /^pg_\w+/ ? $valid++ : $invalid++;
	}
	ok($valid >= 1, q{Statement handle method "private_attribute_info" returns at least one record});
	is($invalid, 0, q{Statement handle method "private_attribute_info" returns only internal names});
	$sth->finish();
}


cleanup_database($dbh,'test');
$dbh->rollback();
$dbh->disconnect();
