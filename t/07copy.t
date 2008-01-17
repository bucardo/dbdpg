#!perl

## Test the COPY functionality

use strict;
use warnings;
use Data::Dumper;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (defined $dbh) {
	plan tests => 54;
}
else {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

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


##
## Test the new COPY methods
##

$dbh->do("DELETE FROM $table");

my $t=q{pg_putcopydata fails if not after a COPY statement};
eval {
	$dbh->pg_putcopydata("pizza\tpie");
};
like($@, qr{COPY command}, $t);

$t=q{pg_getcopydata fails if not after a COPY statement};
eval {
	$dbh->pg_getcopydata($data[0]);
};
like($@, qr{COPY command}, $t);

$t=q{pg_getcopydata_async fails if not after a COPY statement};
eval {
	$dbh->pg_getcopydata_async($data[0]);
};
like($@, qr{COPY command}, $t);

$t=q{pg_putcopyend warns but does not die if not after a COPY statement};
eval { require Test::Warn; };
if ($@) {
	pass('Skipping Test::Warn test');
}
else {
	Test::Warn::warning_like (sub { $dbh->pg_putcopyend(); }, qr/until a COPY/, $t);
}

$t=q{pg_getcopydata does not work if we are using COPY .. FROM};
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
eval {
	$dbh->pg_getcopydata($data[0]);
};
like($@, qr{COPY command}, $t);

$t=q{pg_putcopydata does not work if we are using COPY .. TO};
$dbh->rollback();
$dbh->do("COPY $table TO STDOUT");
eval {
	$dbh->pg_putcopydata("pizza\tpie");
};
like($@, qr{COPY command}, $t);

$t=q{pg_putcopydata works and returns a 1 on success};
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putcopydata("15\tBlueberry");
is ($result, 1, $t);

$t=q{pg_putcopydata works on second call};
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putcopydata("16\tMoreBlueberries");
is ($result, 1, $t);

$t=q{pg_putcopydata fails with invalid data};
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
eval {
	$dbh->pg_putcopydata();
};
ok($@, $t);

$t=q{Calling pg_getcopydata gives an error when in the middle of COPY .. FROM};
eval {
	$dbh->pg_getcopydata($data[0]);
};
like($@, qr{COPY command}, $t);

$t=q{Calling do() gives an error when in the middle of COPY .. FROM};
eval {
	$dbh->do('SELECT 123');
};
like($@, qr{call pg_putcopyend}, $t);

$t=q{pg_putcopydata works after a rude non-COPY attempt};
eval {
	$result = $dbh->pg_putcopydata("17\tMoreBlueberries");
};
is($@, q{}, $t);
is ($result, 1, $t);

$t=q{pg_putcopyend works and returns a 1};
eval {
	$result = $dbh->pg_putcopyend();
};
is($@, q{}, $t);
is ($result, 1, $t);

$dbh->commit();
$t=q{pg_putcopydata fails after pg_putcopyend is called};
eval {
	$result = $dbh->pg_putcopydata('root');
};
like($@, qr{COPY command}, $t);

$t=q{Normal queries work after pg_putcopyend is called};
eval {
	$dbh->do('SELECT 123');
};
is($@, q{}, $t);

$t=q{Data from pg_putcopydata was entered correctly};
$result = $dbh->selectall_arrayref("SELECT id2,val2 FROM $table ORDER BY id2");
$expected = [['12','Mulberry'],['13','Strawberry'],[14,'Blueberry'],[17,'MoreBlueberries']];
is_deeply($result, $expected, $t);

$dbh->do("COPY $table TO STDOUT");
$t=q{pg_getcopydata fails when argument is not a variable};
eval {
	$dbh->pg_getcopydata('wrongo');
};
like($@, qr{read-only}, $t);

$t=q{pg_getcopydata works and returns the length of the string};
$data[0] = 'old';
eval {
	$dbh->pg_getcopydata($data[0]);
};
is($@, q{}, $t);
is($data[0], "13\tStrawberry\n", $t);

$t=q{pg_getcopydata works when argument is a reference};
eval {
	$dbh->pg_getcopydata(\$data[0]);
};
is($@, q{}, $t);
is($data[0], "14\tBlueberry\n", $t);

$t=q{Calling do() gives an error when in the middle of COPY .. TO};
eval {
	$dbh->do('SELECT 234');
};
like($@, qr{pg_getcopydata}, $t);

$t=q{Calling pg_putcopydata gives an errors when in the middle of COPY .. TO};
eval {
	$dbh->pg_putcopydata('pie');
};
like($@, qr{COPY command}, $t);

$t=q{pg_getcopydata returns 0 when no more data};
$dbh->pg_getcopydata(\$data[0]);
eval {
	$result = $dbh->pg_getcopydata(\$data[0]);
};
is($@, q{}, $t);
is($data[0], '', $t);
is($result, -1, $t);

$t=q{Normal queries work after pg_getcopydata runs out};
eval {
	$dbh->do('SELECT 234');
};
is($@, q{}, $t);

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
