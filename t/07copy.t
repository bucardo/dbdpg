#!perl

## Test the COPY functionality

use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

if (defined $ENV{DBI_DSN}) {
	plan tests => 25;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = connect_database();
ok( defined $dbh, 'Connect to database for bytea testing');

my ($sth,$count,$result,$expected,@data);

my $table = 'dbd_pg_test4';
$dbh->do(qq{CREATE TABLE $table(id2 integer, val2 text)});
$dbh->commit();

#
# Test of the pg_putline and pg_endcopy methods
#

## pg_putline should fail unless we are in a COPY IN state
eval {
	$dbh->pg_putline("12\tMulberry");
};
ok($@, 'pg_putline fails when issued without a preceding COPY command');

$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putline("12\tMulberry\n");
is($result,1,'putline returned a value of 1 for success');
$result = $dbh->pg_putline("13\tStrawberry\n");
is($result,1,'putline returned a value of 1 for success');
$result = $dbh->pg_putline("14\tBlueberry\n");
is($result,1,'putline returned a value of 1 for success');

## Commands are not allowed while in a COPY IN state
eval {
	$dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok($@, 'do() fails while in a COPY IN state');

## pg_getline is not allowed as we are in a COPY_IN state
$data[0] = '';
eval {
	$dbh->pg_getline($data[0], 100);
};
ok($@, 'pg_getline fails while in a COPY IN state');

$result = $dbh->pg_endcopy();
is($result,1,'pg_endcopy returned a 1');

## Make sure we can issue normal commands again
$dbh->do(q{SELECT 'dbdpg_copytest'});

## Make sure we are out of the COPY IN state and pg_putline no longer works
eval {
	$dbh->pg_putline("16\tBlackberry");
};
ok($@, 'pg_putline fails when issued after pg_endcopy called');

## Check that our lines were inserted properly
$expected = [[12 => 'Mulberry'],[13 => 'Strawberry'],[14 => 'Blueberry']];
$result = $dbh->selectall_arrayref("SELECT id2,val2 FROM $table ORDER BY id2");
is_deeply( $result, $expected, 'putline inserted values correctly');

# pg_endcopy should not work because we are no longer in a COPY state
eval {
	$dbh->pg_endcopy;
};
ok($@, 'pg_endcopy fails when called twice after COPY IN');

$dbh->commit();

#
# Test of the pg_getline method
#

## pg_getline should fail unless we are in a COPY OUT state
eval {
	$dbh->pg_getline($data[0], 100);
};
ok($@, 'pg_getline fails when issued without a preceding COPY command');

$dbh->do("COPY $table TO STDOUT");
my ($buffer,$badret,$badval) = ('',0,0);
$result = $dbh->pg_getline($data[0], 100);
is ($result, 1, 'pg_getline returned a 1');

## Commands are not allowed while in a COPY OUT state
eval {
	$dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok($@, 'do() fails while in a COPY OUT state');

## pg_putline is not allowed as we are in a COPY OUT state
eval {
	$dbh->pg_putline("99\tBogusberry");
};
ok($@, 'pg_putline fails while in a COPY OUT state');

$data[1]=$data[2]=$data[3]='';
$result = $dbh->pg_getline($data[1], 100);
is ($result, 1, 'pg_getline returned a 1');
$result = $dbh->pg_getline($data[2], 100);
is ($result, 1, 'pg_getline returned a 1');

$result = $dbh->pg_getline($data[3], 100);
is ($result, '', 'pg_getline returns empty on final call');

$result = \@data;
$expected = ["12\tMulberry\n","13\tStrawberry\n","14\tBlueberry\n",''];
is_deeply( $result, $expected, 'getline returned all rows successfuly');

## Make sure we can issue normal commands again
$dbh->do(q{SELECT 'dbdpg_copytest'});

## Make sure we are out of the COPY OUT state and pg_getline no longer works
eval {
	$data[5]='';
	$dbh->pg_getline($data[5], 100);
};
ok($@, 'pg_getline fails when issued after pg_endcopy called');

## pg_endcopy should fail because we are no longer in a COPY state
eval {
	$dbh->pg_endcopy;
};
ok($@, 'pg_endcopy fails when called twice after COPY OUT');


#
# Make sure rollback and commit reset our internal copystate tracking
#

$dbh->do("COPY $table TO STDOUT");
$dbh->commit();
eval {
	$dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok(!$@, 'commit resets COPY state');

$dbh->do("COPY $table TO STDOUT");
$dbh->rollback();
eval {
	$dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok(!$@, 'rollback resets COPY state');


#
# Keep old-style calls around for backwards compatibility
#

$dbh->do("COPY $table FROM STDIN");
$result = $dbh->func("13\tOlive\n", 'putline');
is ($result, 1, q{old-style dbh->func('text', 'putline') still works});
$dbh->pg_endcopy;

$dbh->do("COPY $table TO STDOUT");
$result = $dbh->func($data[0], 100, 'getline');
is ($result, 1, q{old-style dbh->func(var, length, 'getline') still works});
1 while ($result = $dbh->func($data[0], 100, 'getline'));

$dbh->do("DROP TABLE $table");
$dbh->commit();

cleanup_database($dbh,'test');
$dbh->disconnect;
