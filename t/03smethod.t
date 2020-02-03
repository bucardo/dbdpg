#!perl

## Test of the statement handle methods
## The following methods are *not* currently tested here:
## "execute"
## "finish"
## "dump_results"

use 5.008001;
use strict;
use warnings;
use POSIX qw(:signal_h);
use Test::More;
use DBI ':sql_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 128;

isnt ($dbh, undef, 'Connect to database for statement handle method testing');

my ($SQL, $sth, $sth2, $result, @result, $expected, $rows, $t);

#
# Test of the prepare flags
#

$t=q{Calling prepare() with no arguments gives an error};
eval{ $sth = $dbh->prepare(); };
like ($@, qr{\+ 0}, $t);

$t=q{Calling prepare() with an undefined value returns undef};
$sth = $dbh->prepare(undef);
is ($sth, undef, $t);

$t='Prepare/execute with no flags works';
$SQL = 'SELECT id FROM dbd_pg_test WHERE id = ?';
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_server_prepare off at database handle works';
$dbh->{pg_server_prepare} = 0;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok ($sth->execute, $t);

$t='Setting database attribute pg_switch_prepared to 7 works';
$dbh->{pg_switch_prepared} = 7;
is ($dbh->{pg_switch_prepared}, 7, $t);

$t='Statement handle inherits pg_switch_prepared setting';
$sth = $dbh->prepare($SQL);
is ($sth->{pg_switch_prepared}, 7, $t);

$t='Setting statement attribute pg_switch_prepared to 6 works';
$sth->{pg_switch_prepared} = 6;
is ($sth->{pg_switch_prepared}, 6, $t);

$t='Running with statement attribute pg_switch_prepared at 6 works';
for (1..10) {
    $sth->execute(1);
    my $it = "$t (run $_ of 10)";
    ok ($sth->execute, $it);
}

$t='Running with statement attribute pg_switch_prepared at -1 works';
$sth->{pg_switch_prepared} = -1;
for (1..4) {
    $sth->execute(1);
    my $it = "$t (run $_ of 4)";
    ok ($sth->execute, $it);
}

$t='Running with statement attribute pg_switch_prepared at 0 works';
$sth->{pg_switch_prepared} = 0;
for (1..4) {
    $sth->execute(1);
    my $it = "$t (run $_ of 4)";
    ok ($sth->execute, $it);
}

$t='Running with statement attribute pg_switch_prepared at 1 works';
$sth->{pg_switch_prepared} = 1;
for (1..4) {
    $sth->execute(1);
    my $it = "$t (run $_ of 4)";
    ok ($sth->execute, $it);
}

$t='Prepare/execute with pg_server_prepare on at database handle works';
$dbh->{pg_server_prepare} = 1;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok ($sth->execute, $t);

## We must send a hashref as the final arg
$t='Prepare failes when sent a non-hashref';
eval {
    $sth = $dbh->prepare('SELECT 123', ['I am not a hashref!']);
};
like ($@, qr{not a hash}, $t);


# Make sure that undefs are converted to NULL.
$t='Prepare/execute with undef converted to NULL';
$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, pdate) VALUES (?,?)');
ok ($sth->execute(401, undef), $t);

$t='Prepare/execute with pg_server_prepare off at statement handle works';
$sth = $dbh->prepare($SQL, {pg_server_prepare => 0});
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_server_prepare on at statement handle works';
$sth = $dbh->prepare($SQL, {pg_server_prepare => 1});
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_prepare_now on at database handle works';
$dbh->{pg_prepare_now} = 1;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_prepare_now off at database handle works';
$dbh->{pg_prepare_now} = 0;
$sth = $dbh->prepare($SQL);
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_prepare_now off at statement handle works';
$sth = $dbh->prepare($SQL, {pg_prepare_now => 0});
$sth->execute(1);
ok ($sth->execute, $t);

$t='Prepare/execute with pg_prepare_now on at statement handle works';
$sth = $dbh->prepare($SQL, {pg_prepare_now => 1});
$sth->execute(1);
ok ($sth->execute, $t);

# Test using our own prepared statements
$t='Prepare/execute works with pg_prepare_name';
my $pgversion = $dbh->{pg_server_version};
my $myname = 'dbdpg_test_1';
$dbh->do("PREPARE $myname(int) AS SELECT COUNT(*) FROM pg_class WHERE reltuples > \$1", {pg_direct=> 1});
$sth = $dbh->prepare('SELECT ?');
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->{pg_prepare_name} = $myname;
ok ($sth->execute(1), $t);
$dbh->do("DEALLOCATE $myname");


#
# Test of the "bind_param" statement handle method
#

$t='Statement handle method "bind_param" works when binding an int column with an int';
$SQL = 'SELECT id FROM dbd_pg_test WHERE id = ?';
$sth = $dbh->prepare($SQL);
ok ($sth->bind_param(1, 1), $t);

$t='Statement handle method "bind_param" works when rebinding an int column with a string';
ok ($sth->bind_param(1, 'foo'), $t);

# Check if the server is sending us warning messages
# We assume that older servers are okay
my $client_level = '';
$sth2 = $dbh->prepare('SHOW client_min_messages');
$sth2->execute();
$client_level = $sth2->fetchall_arrayref()->[0][0];

#
# Test of the "bind_param_inout" statement handle method
#

$t='Invalid placeholder fails for bind_param_inout';
my $var = 123;
$sth = $dbh->prepare('SELECT 1+?::int');
eval { $sth->bind_param_inout(0, \$var, 0); };
like ($@, qr{Cannot bind}, $t);

eval { $sth->bind_param_inout(3, \$var, 0); };
like ($@, qr{Cannot bind}, $t);

$t = q{Calling bind_param_inout with a non-scalar reference fails};
eval { $sth->bind_param_inout(1, 'noway', 0); };
like ($@, qr{needs a reference}, $t);

eval { $sth->bind_param_inout(1, $t, 0); };
like ($@, qr{needs a reference}, $t);

eval { $sth->bind_param_inout(1, [123], 0); };
like ($@, qr{needs a reference}, $t);


$t = q{Calling bind_param_inout changes an integer value};
eval { $sth->bind_param_inout(1, \$var, 0); };
is ($@, q{}, $t);
$var = 999;
$sth->execute();
$sth->fetch;
is ($var, 1000, $t);

$t = q{Calling bind_param_inout changes a string value};
$sth = $dbh->prepare(q{SELECT 'X'||?::text});
$sth->bind_param_inout(1, \$var, 0);
$var = 'abc';
$sth->execute();
$sth->fetch;
is ($var, 'Xabc', $t);

$t = q{Calling bind_param_inout changes a string to a float};
$sth = $dbh->prepare('SELECT ?::float');
$sth->bind_param_inout(1, \$var, 0);
$var = '1e+6';
$sth->execute();
$sth->fetch;
is ($var, '1000000', $t);

$t = q{Calling bind_param_inout works for second placeholder};
$sth = $dbh->prepare('SELECT ?::float, 1+?::int');
$sth->bind_param_inout(2, \$var, 0);
$var = 111;
$sth->execute(222,333);
$sth->fetch;
is ($var, 112, $t);

$t = q{Calling bind_param_inout changes two variables at once};
my $var2 = 234;
$sth = $dbh->prepare('SELECT 1+?::float, 1+?::int');
$sth->bind_param_inout(1, \$var, 0);
$sth->bind_param_inout(2, \$var2, 0);
$var = 444; $var2 = 555;
$sth->execute();
$sth->fetch;
is ($var, 445, $t);
is ($var2, 556, $t);

#
# Test of the "bind_param_array" statement handle method
#

$sth = $dbh->prepare('INSERT INTO dbd_pg_test (id, val) VALUES (?,?)');
# Try with 1, 2, and 3 values. All should succeed

$t='Statement handle method "bind_param_array" fails if second arg is a hashref';
eval {
    $sth->bind_param_array(1, {}, SQL_INTEGER);
};
like ($@, qr{must be a scalar or an arrayref}, $t);

$t='Statement handle method "bind_param_array" fails if first arg is not a number';
eval {
    $sth->bind_param_array('bread pudding', 123, SQL_INTEGER);
};
like ($@, qr{named placeholders}, $t);

$t='Statement handle method "bind_param_array" works binding three values to the first placeholder';
eval {
    $sth->bind_param_array(1, [ 30, 31, 32 ], SQL_INTEGER);
};
is ($@, q{}, $t);

$t='Statement handle method "bind_param_array" works binding one scalar value to the second placeholder';
eval {
    $sth->bind_param_array(2, 'Mulberry');
};
is ($@, q{}, $t);

$t='Statement handle method "bind_param_array" works binding three values to the second placeholder';
eval {
    $sth->bind_param_array(2, [ 'Mango', 'Strawberry', 'Gooseberry' ]);
};
is ($@, q{}, $t);

$t='Statement handle method "bind_param_array" works when binding one value to the second placeholder';
eval {
    $sth->bind_param_array(2, [ 'Mangoz' ]);
};
is ($@, q{}, $t);

$t='Statement handle method "bind_param_array" works when binding two values to the second placeholder';
eval {
    $sth->bind_param_array(2, [ 'Plantain', 'Apple' ]);
};
is ($@, q{}, $t);

#
# Test of the "execute_array" statement handle method
#

$t='Statement method handle "execute_array" works';
$dbh->{RaiseError}=1;
my @tuple_status;
$rows = $sth->execute_array( { ArrayTupleStatus => \@tuple_status });
is_deeply (\@tuple_status, [1,1,1], $t);

$t='Statement method handle "execute_array" returns correct number of rows';
is ($rows, 3, $t);

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

$t='Statement method handle "execute_array" works with ArrayTupleFetch';
undef @tuple_status;
$rows = $sth->execute_array( { ArrayTupleStatus => \@tuple_status, ArrayTupleFetch => \&getval });
is_deeply (\@tuple_status, [1,1,1,1], $t);

$t='Statement method handle "execute_array" returns correct number of rows with ArrayTupleFetch';
is ($rows, 4, $t);

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


$t='Statement handle method "execute_for_fetch" works';
is_deeply (\@tuple_status, [map{1}(1..$goodrows)], $t);

$t='Statement handle method "execute_for_fetch" returns correct number of rows';
is ($rows, $goodrows, $t);

#
# Test of the "fetchrow_arrayref" statement handle method
#

$t='Statement handle method "fetchrow_arrayref" returns first row correctly';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id = 34');
$sth->execute();
$result = $sth->fetchrow_arrayref();
is_deeply ($result, [34, 'Huckleberry'], $t);

$t='Statement handle method "fetchrow_arrayref" returns undef when done';
$result = $sth->fetchrow_arrayref();
is_deeply ($result, undef, $t);

# Test of the "fetch" alias
$t='Statement handle method alias "fetch" returns first row correctly';
$sth->execute();
$result = $sth->fetch();
$expected = [34, 'Huckleberry'];
is_deeply ($result, $expected, $t);

$t='Statement handle method alias "fetch" returns undef when done';
$result = $sth->fetch();
is_deeply ($result, undef, $t);

#
# Test of the "fetchrow_array" statement handle method
#

$t='Statement handle method "fetchrow_array" returns first row correctly';
$sth->execute();
@result = $sth->fetchrow_array();
is_deeply (\@result, $expected, $t);

$t='Statement handle method "fetchrow_array" returns an empty list when done';
@result = $sth->fetchrow_array();
is_deeply (\@result, [], $t);

#
# Test of the "fetchrow_hashref" statement handle method
#

$t='Statement handle method "fetchrow_hashref" works with a slice argument';
$sth->execute();
$result = $sth->fetchrow_hashref();
$expected = {id => 34, val => 'Huckleberry'};
is_deeply ($result, $expected, $t);

$t='Statement handle method "fetchrow_hashref" returns undef when done';
$result = $sth->fetchrow_hashref();
is_deeply ($result, undef, $t);

#
# Test of the "fetchall_arrayref" statement handle method
#

$t='Statement handle method "fetchall_arrayref" returns first row correctly';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (35,36) ORDER BY id ASC');
$sth->execute();
$result = $sth->fetchall_arrayref();
$expected = [[35,'Guava'],[36,'Lemon']];
is_deeply ($result, $expected, $t);

# Test of the 'slice' argument

$t='Statement handle method "fetchall_arrayref" works with an arrayref slice';
$sth->execute();
$result = $sth->fetchall_arrayref([1]);
$expected = [['Guava'],['Lemon']];
is_deeply ($result, $expected, $t);

$t='Statement handle method "fetchall_arrayref" works with a hashref slice';
$sth->execute();
$result = $sth->fetchall_arrayref({id => 1});
$expected = [{id => 35},{id => 36}];
is_deeply ($result, $expected, $t);

# My personal favorite way of grabbing data
$t='Statement handle method "fetchall_arrayref" works with an empty hashref slice';
$sth->execute();
$result = $sth->fetchall_arrayref({});
$expected = [{id => 35, val => 'Guava'},{id => 36, val => 'Lemon'}];
is_deeply ($result, $expected, $t);


SKIP: {
    if ($DBI::VERSION >= 1.603) {
        skip ('fetchall_arrayref max rows broken in DBI 1.603', 2);
    }

    # Test of the 'maxrows' argument
    $t=q{Statement handle method "fetchall_arrayref" works with a 'maxrows' argument};
    $sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id >= 33 ORDER BY id ASC LIMIT 10');
    $sth->execute();
    $result = $sth->fetchall_arrayref(undef,2);
    $expected = [[33,'Peach'],[34,'Huckleberry']];
    is_deeply ($result, $expected, $t);

    $t=q{Statement handle method "fetchall_arrayref" works with an arrayref slice and a 'maxrows' argument};
    $result = $sth->fetchall_arrayref([1],2);
    $expected = [['Guava'],['Lemon']];
    $sth->finish();
    is_deeply ($result, $expected, $t);
}

#
# Test of the "fetchall_hashref" statement handle method
#

$t='Statement handle method "fetchall_hashref" gives an error when called with no arguments';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
eval {
    $sth->fetchall_hashref();
};
isnt ($@, q{}, $t);

$t='Statement handle method "fetchall_hashref" works with a named key field';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
$result = $sth->fetchall_hashref('id');
$expected = {33=>{id => 33, val => 'Peach'},34=>{id => 34, val => 'Huckleberry'}};
is_deeply ($result, $expected, $t);

$t='Statement handle method "fetchall_hashref" returns an empty hash when no rows returned';
$sth->execute();
$result = $sth->fetchall_hashref(1);
is_deeply ($result, $expected, q{Statement handle method "fetchall_hashref" works with a numeric key field});
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id < 1');
$sth->execute();
$result = $sth->fetchall_hashref(1);
is_deeply ($result, {}, $t);

#
# Test of the "rows" statement handle method
#

$t='Statement handle method "rows" returns -1 before an execute';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$rows = $sth->rows();
is ($rows, -1, $t);

$t='Statement handle method "rows" returns correct number of rows';
$sth->execute();
$rows = $sth->rows();
$sth->finish();
is ($rows, 2, $t);

#
# Test of the "bind_col" statement handle method
#

$t='Statement handle method "bind_col" returns the correct value';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34)');
$sth->execute();
my $bindme;
$result = $sth->bind_col(2, \$bindme);
is ($result, 1, $t);

$t='Statement handle method "bind_col" correctly binds parameters';
$sth->fetch();
is ($bindme, 'Peach', $t);

$dbh->do(q{UPDATE dbd_pg_test SET testarray = '{2,3,55}' WHERE id = 33});

$t='Statement handle method "bind_col" returns the correct value';
my $bindarray;
$sth = $dbh->prepare('SELECT id, testarray FROM dbd_pg_test WHERE id = 33');
$sth->execute();
$result = $sth->bind_col(1, \$bindme);
is ($result, 1, $t);

$t='Statement handle method "bind_col" returns the correct value';
$result = $sth->bind_col(2, \$bindarray);
is ($result, 1, $t);

$t='Statement handle method "bind_col" correctly binds parameters';
$sth->fetch();
is ($bindme, '33', $t);

$t='Statement handle method "bind_col" correctly binds arrayref';
is_deeply ($bindarray, [2,3,55], $t);


#
# Test of the "bind_columns" statement handle method
#

$t='Statement handle method "bind_columns" fails when called with wrong number of arguments';
$sth = $dbh->prepare('SELECT id, val FROM dbd_pg_test WHERE id IN (33,34) ORDER BY id');
$sth->execute();
my $bindme2;
eval {
    $sth->bind_columns(1);
};
isnt ($@, q{}, $t);

$t='Statement handle method "bind_columns" returns the correct value';
$result = $sth->bind_columns(\$bindme, \$bindme2);
is ($result, 1, $t);

$t='Statement handle method "bind_columns" correctly binds parameters';
$sth->fetch();
$expected = [33, 'Peach'];
my $got = [$bindme, $bindme2];
$sth->finish();
is_deeply ($got, $expected, $t);

#
# Test of the statement handle method "state"
#

$t='Statement handle method "state" returns an empty string on success';
$result = $sth->state();
is ($result, q{}, $t);

$t='Statement handle method "state" returns a five-character code on error';
eval {
    $sth = $dbh->prepare('SELECT dbdpg_throws_an_error');
    $sth->execute();
};
$result = $sth->state();
like ($result, qr/^[A-Z0-9]{5}$/, $t);

$t='Statement and database handle method "state" return same code';
my $result2 = $dbh->state();
is ($result, $result2, $t);

$t='Statement handle method "state" returns expected code';
is ($result, '42703', $t);

#
# Test of the statement handle method "private_attribute_info"
#

SKIP: {
    if ($DBI::VERSION < 1.54) {
        skip ('DBI must be at least version 1.54 to test private_attribute_info', 2);
    }


    $t='Statement handle method "private_attribute_info" returns at least one record';
    $sth = $dbh->prepare('SELECT 123');
    my $private = $sth->private_attribute_info();
    my ($valid,$invalid) = (0,0);
    for my $name (keys %$private) {
        $name =~ /^pg_\w+/ ? $valid++ : $invalid++;
    }
    cmp_ok ($valid, '>=', 1, $t);

    $t='Statement handle method "private_attribute_info" returns only internal names';
    $sth->finish();
    is ($invalid, 0, $t);
}


#
# Test of the statement handle method "pg_numbound"
#

$dbh->rollback();
$t=q{Statement handle attribute pg_numbound returns 0 if no placeholders};
$sth = $dbh->prepare('SELECT 123');
is ($sth->{pg_numbound}, 0, $t);

$sth->execute();
is ($sth->{pg_numbound}, 0, $t);

$t=q{Statement handle attribute pg_numbound returns 0 if no placeholders bound yet};
$sth = $dbh->prepare('SELECT 123 WHERE 1 > ? AND 2 > ?');
is ($sth->{pg_numbound}, 0, $t);

$t=q{Statement handle attribute pg_numbound returns 1 if one placeholder bound};
$sth->bind_param(1, 123);
is ($sth->{pg_numbound}, 1, $t);

$t=q{Statement handle attribute pg_numbound returns 2 if two placeholders bound};
$sth->bind_param(2, 345);
is ($sth->{pg_numbound}, 2, $t);

$t=q{Statement handle attribute pg_numbound returns 1 if one placeholders bound as NULL};
$sth = $dbh->prepare('SELECT 123 WHERE 1 > ? AND 2 > ?');
$sth->bind_param(1, undef);
is ($sth->{pg_numbound}, 1, $t);

#
# Test of the statement handle method "pg_bound"
#

$t=q{Statement handle attribute pg_bound returns an empty hash if no placeholders};
$sth = $dbh->prepare('SELECT 123');
is_deeply ($sth->{pg_bound}, {}, $t);

$sth->execute();
is_deeply ($sth->{pg_bound}, {}, $t);

$t=q{Statement handle attribute pg_bound returns correct value if no placeholders bound yet};
$sth = $dbh->prepare('SELECT 123 WHERE 1 > ? AND 2 > ?');
is_deeply ($sth->{pg_bound}, {1=>0, 2=>0}, $t);

$t=q{Statement handle attribute pg_bound returns correct value if one placeholder bound};
$sth->bind_param(2, 123);
is_deeply ($sth->{pg_bound}, {1=>0, 2=>1}, $t);

$t=q{Statement handle attribute pg_bound returns correct value if two placeholders bound};
$sth->bind_param(1, 123);
is_deeply ($sth->{pg_bound}, {1=>1, 2=>1}, $t);

#
# Test of the statement handle method "pg_numbound"
#

$t=q{Statement handle attribute pg_numbound returns 1 if one placeholders bound as NULL};
$sth = $dbh->prepare('SELECT 123 WHERE 1 > ? AND 2 > ?');
$sth->bind_param(1, undef);
is_deeply ($sth->{pg_bound}, {1=>1, 2=>0}, $t);


#
# Test of the statement handle method "pg_current_row"
#

$t=q{Statement handle attribute pg_current_row returns zero until first row fetched};
$sth = $dbh->prepare('SELECT 1 FROM pg_class LIMIT 5');
is ($sth->{pg_current_row}, 0, $t);

$t=q{Statement handle attribute pg_current_row returns zero until first row fetched};
$sth->execute();
is ($sth->{pg_current_row}, 0, $t);

$t=q{Statement handle attribute pg_current_row returns 1 after a fetch};
$sth->fetch();
is ($sth->{pg_current_row}, 1, $t);

$t=q{Statement handle attribute pg_current_row returns correct value while fetching};
my $x = 2;
while (defined $sth->fetch()) {
    is ($sth->{pg_current_row}, $x++, $t);
}
$t=q{Statement handle attribute pg_current_row returns 0 when done fetching};
is ($sth->{pg_current_row}, 0, $t);

$t=q{Statement handle attribute pg_current_row returns 0 after fetchall_arrayref};
$sth->execute();
$sth->fetchall_arrayref();
is ($sth->{pg_current_row}, 0, $t);

#
# Test of the statement handle method "cancel"
#

SKIP: {
    if ($^O =~ /Win/) {
        skip ('Cannot test POSIX signalling on Windows', 1);
    }

    $dbh->do('INSERT INTO dbd_pg_test (id) VALUES (?)',undef,1);
    $dbh->commit;
    $dbh->do('SELECT * FROM dbd_pg_test WHERE id = ? FOR UPDATE',undef,1);

    my $dbh2 = $dbh->clone;
    $dbh2->do('SET search_path TO ' . $dbh->selectrow_array('SHOW search_path'));

    my $oldaction;
    eval {
        # This statement will block indefinitely because of the 'FOR UPDATE' clause,
        # so we set up an alarm to cancel it after 2 seconds.
        my $sthl = $dbh2->prepare('SELECT * FROM dbd_pg_test WHERE id = ? FOR UPDATE');
        $sthl->{RaiseError} = 1;

        my $action = POSIX::SigAction->new(
            sub {$sthl->cancel},POSIX::SigSet->new(SIGALRM));
        $oldaction = POSIX::SigAction->new;
        POSIX::sigaction(SIGALRM,$action,$oldaction);

        alarm(2); # seconds before alarm
        $sthl->execute(1);
        alarm(0); # cancel alarm (if execute didn't block)
    };
    # restore original signal handler
    POSIX::sigaction(SIGALRM,$oldaction);
    like ($@, qr/execute failed/, 'cancel');
    $dbh2->disconnect();
}

#
# Test of the statement handle methods "pg_canonical_names"
#

$t=q{Statement handle method "pg_canonical_names" returns expected values};
$sth = $dbh->prepare('SELECT id, id AS not_id, id + 1 AS not_a_simple FROM dbd_pg_test LIMIT 1');
$sth->execute;

is_deeply ($sth->pg_canonical_names, [
    'dbd_pg_testschema.dbd_pg_test.id',
    'dbd_pg_testschema.dbd_pg_test.id',
    undef
], $t);

#
# Test of the statement handle methods "pg_canonical_ids"
#

$t=q{Statement handle method "pg_canonical_ids" returns correct length};
my $data = $sth->pg_canonical_ids;
is ($#$data, 2, $t);

$t=q{Statement handle method pg_canonical_ids has undef as the last element in returned array};
is ($data->[2], undef, $t);

$t=q{Statement handle method "pg_canonical_ids" returns identical first and second elements};
$t=q{first and second array elements must be the same};
is_deeply ($data->[0], $data->[1], $t);

$sth->finish;


cleanup_database($dbh,'test');
$dbh->rollback();
$dbh->disconnect();
