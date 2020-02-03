#!perl

## Test the COPY functionality

use 5.008001;
use strict;
use warnings;
use Data::Dumper;
use DBD::Pg ':async';
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if ($dbh) {
    plan tests => 62;
}
else {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

ok (defined $dbh, 'Connect to database for COPY testing');

my ($result,$expected,@data,$t);

my $table = 'dbd_pg_test4';
$dbh->do(qq{CREATE TABLE $table(id2 integer, val2 text)});
$dbh->commit();
my $pgversion = $dbh->{pg_server_version};

#
# Test of the pg_putline and pg_endcopy methods
#

## pg_putline should fail unless we are in a COPY IN state
$t='pg_putline fails when issued without a preceding COPY command';
eval {
    $dbh->pg_putline("12\tMulberry");
};
ok ($@, $t);

$t='putline returned a value of 1 for success';
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putline("12\tMulberry\n");
is ($result, 1, $t);

$t='putline returned a value of 1 for success';
$result = $dbh->pg_putline("13\tStrawberry\n");
is ($result, 1, $t);

$t='putline returned a value of 1 for success';
$result = $dbh->pg_putline("14\tBlueberry\n");
is ($result, 1, $t);

## Commands are not allowed while in a COPY IN state
$t='do() fails while in a COPY IN state';
eval {
    $dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok ($@, $t);

## pg_getline is not allowed as we are in a COPY_IN state
$t='pg_getline fails while in a COPY IN state';
$data[0] = '';
eval {
    $dbh->pg_getline($data[0], 100);
};
ok ($@, $t);

$t='pg_endcopy returned a 1';
$result = $dbh->pg_endcopy();
is ($result, 1, $t);

## Make sure we can issue normal commands again
$dbh->do(q{SELECT 'dbdpg_copytest'});

## Make sure we are out of the COPY IN state and pg_putline no longer works
$t='pg_putline fails when issued after pg_endcopy called';
eval {
    $dbh->pg_putline("16\tBlackberry");
};
ok ($@, $t);

## Check that our lines were inserted properly
$t='putline inserted values correctly';
$expected = [[12 => 'Mulberry'],[13 => 'Strawberry'],[14 => 'Blueberry']];
$result = $dbh->selectall_arrayref("SELECT id2,val2 FROM $table ORDER BY id2");
is_deeply ($result, $expected, $t);

# pg_endcopy should not work because we are no longer in a COPY state
$t='pg_endcopy fails when called twice after COPY IN';
eval {
    $dbh->pg_endcopy;
};
ok ($@, $t);

$dbh->commit();

#
# Test of the pg_getline method
#

## pg_getline should fail unless we are in a COPY OUT state
$t='pg_getline fails when issued without a preceding COPY command';
eval {
    $dbh->pg_getline($data[0], 100);
};
ok ($@, $t);

$t='pg_getline returns a 1';
$dbh->do("COPY $table TO STDOUT");
my ($buffer,$badret,$badval) = ('',0,0);
$result = $dbh->pg_getline($data[0], 100);
is ($result, 1, $t);

## Commands are not allowed while in a COPY OUT state
$t='do() fails while in a COPY OUT state';
eval {
    $dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok ($@, $t);

## pg_putline is not allowed as we are in a COPY OUT state
$t='pg_putline fails while in a COPY OUT state';
eval {
    $dbh->pg_putline("99\tBogusberry");
};
ok ($@, $t);

$t='pg_getline returned a 1';
$data[1]=$data[2]=$data[3]='';
$result = $dbh->pg_getline($data[1], 100);
is ($result, 1, $t);

$t='pg_getline returned a 1';
$result = $dbh->pg_getline($data[2], 100);
is ($result, 1, $t);

$t='pg_getline returns empty on final call';
$result = $dbh->pg_getline($data[3], 100);
is ($result, '', $t);

$t='getline returned all rows successfuly';
$result = \@data;
$expected = ["12\tMulberry\n","13\tStrawberry\n","14\tBlueberry\n",''];
is_deeply ($result, $expected, $t);

## Make sure we can issue normal commands again
$dbh->do(q{SELECT 'dbdpg_copytest'});

## Make sure we are out of the COPY OUT state and pg_getline no longer works
$t='pg_getline fails when issued after pg_endcopy called';
eval {
    $data[5]='';
    $dbh->pg_getline($data[5], 100);
};
ok ($@, $t);

## pg_endcopy should fail because we are no longer in a COPY state
$t='pg_endcopy fails when called twice after COPY OUT';
eval {
    $dbh->pg_endcopy;
};
ok ($@, $t);


##
## Test the new COPY methods
##

$dbh->do("DELETE FROM $table");

$t='pg_putcopydata fails if not after a COPY FROM statement';
eval {
    $dbh->pg_putcopydata("pizza\tpie");
};
like ($@, qr{COPY FROM command}, $t);

$t='pg_getcopydata fails if not after a COPY TO statement';
eval {
    $dbh->pg_getcopydata($data[0]);
};
like ($@, qr{COPY TO command}, $t);

$t='pg_getcopydata_async fails if not after a COPY TO statement';
eval {
    $dbh->pg_getcopydata_async($data[0]);
};
like ($@, qr{COPY TO command}, $t);

$t='pg_putcopyend warns but does not die if not after a COPY statement';
eval { require Test::Warn; };
if ($@) {
    pass ('Skipping Test::Warn test');
}
else {
    Test::Warn::warning_like (sub { $dbh->pg_putcopyend(); }, qr/until a COPY/, $t);
}

$t='pg_getcopydata does not work if we are using COPY .. TO';
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
eval {
    $dbh->pg_getcopydata($data[0]);
};
like ($@, qr{COPY TO command}, $t);

$t='pg_putcopydata does not work if we are using COPY .. FROM';
$dbh->rollback();
$dbh->do("COPY $table TO STDOUT");
eval {
    $dbh->pg_putcopydata("pizza\tpie");
};
like ($@, qr{COPY FROM command}, $t);

$t='pg_putcopydata works and returns a 1 on success';
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putcopydata("15\tBlueberry");
is ($result, 1, $t);

$t='pg_putcopydata works on second call';
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->pg_putcopydata("16\tMoreBlueberries");
is ($result, 1, $t);

$t='pg_putcopydata fails with invalid data';
$dbh->rollback();
$dbh->do("COPY $table FROM STDIN");
eval {
    $dbh->pg_putcopydata();
};
ok ($@, $t);

$t='Calling pg_getcopydata gives an error when in the middle of COPY .. TO';
eval {
    $dbh->pg_getcopydata($data[0]);
};
like ($@, qr{COPY TO command}, $t);

$t='Calling do() gives an error when in the middle of COPY .. FROM';
eval {
    $dbh->do('SELECT 123');
};
like ($@, qr{call pg_putcopyend}, $t);

$t='pg_putcopydata works after a rude non-COPY attempt';
eval {
    $result = $dbh->pg_putcopydata("17\tMoreBlueberries");
};
is ($@, q{}, $t);
is ($result, 1, $t);

$t='pg_putcopyend works and returns a 1';
eval {
    $result = $dbh->pg_putcopyend();
};
is ($@, q{}, $t);
is ($result, 1, $t);

$t='pg_putcopydata fails after pg_putcopyend is called';
$dbh->commit();
eval {
    $result = $dbh->pg_putcopydata('root');
};
like ($@, qr{COPY FROM command}, $t);

$t='Normal queries work after pg_putcopyend is called';
eval {
    $dbh->do('SELECT 123');
};
is ($@, q{}, $t);

$t='Data from pg_putcopydata was entered correctly';
$result = $dbh->selectall_arrayref("SELECT id2,val2 FROM $table ORDER BY id2");
$expected = [['12','Mulberry'],['13','Strawberry'],[14,'Blueberry'],[17,'MoreBlueberries']];
is_deeply ($result, $expected, $t);

$t='pg_getcopydata fails when argument is not a variable';
$dbh->do("COPY $table TO STDOUT");
eval {
    $dbh->pg_getcopydata('wrongo');
};
like ($@, qr{read-only}, $t);

$t='pg_getcopydata works and returns the length of the string';
$data[0] = 'old';
eval {
    $dbh->pg_getcopydata($data[0]);
};
is ($@, q{}, $t);
is ($data[0], "13\tStrawberry\n", $t);

$t='pg_getcopydata works when argument is a reference';
eval {
    $dbh->pg_getcopydata(\$data[0]);
};
is ($@, q{}, $t);
is ($data[0], "14\tBlueberry\n", $t);

$t='Calling do() gives an error when in the middle of COPY .. TO';
eval {
    $dbh->do('SELECT 234');
};
like ($@, qr{pg_getcopydata}, $t);

$t='Calling pg_putcopydata gives an errors when in the middle of COPY .. FROM';
eval {
    $dbh->pg_putcopydata('pie');
};
like ($@, qr{COPY FROM command}, $t);

$t='pg_getcopydata returns 0 when no more data';
$dbh->pg_getcopydata(\$data[0]);
eval {
    $result = $dbh->pg_getcopydata(\$data[0]);
};
is ($@, q{}, $t);
is ($data[0], '', $t);
is ($result, -1, $t);

$t='Normal queries work after pg_getcopydata runs out';
eval {
    $dbh->do('SELECT 234');
};
is ($@, q{}, $t);

$t='Async queries work after COPY OUT';
$dbh->do('CREATE TEMP TABLE foobar AS SELECT 123::INTEGER AS x');
$dbh->do('COPY foobar TO STDOUT');
1 while ($dbh->pg_getcopydata($buffer) >= 0);

eval {
    $dbh->do('SELECT 111', { pg_async => PG_ASYNC} );
};
is ($@, q{}, $t);
$dbh->pg_result();

$t='Async queries work after COPY IN';
$dbh->do('COPY foobar FROM STDIN');
$dbh->pg_putcopydata(456);
$dbh->pg_putcopyend();

eval {
    $dbh->do('SELECT 222', { pg_async => PG_ASYNC} );
};
is ($@, q{}, $t);
$dbh->pg_result();


SKIP: {
    $pgversion < 80200 and skip ('Server version 8.2 or greater needed for test', 1);

    $t='pg_getcopydata works when pulling from an empty table into an empty var';
    $dbh->do(q{COPY (SELECT 1 FROM pg_class LIMIT 0) TO STDOUT});
    eval {
        my $newvar;
        $dbh->pg_getcopydata($newvar);
    };
    is ($@, q{}, $t);
}

#
# Make sure rollback and commit reset our internal copystate tracking
#

$t='commit resets COPY state';
$dbh->do("COPY $table TO STDOUT");
$dbh->commit();
eval {
    $dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok (!$@, $t);

$t='rollback resets COPY state';
$dbh->do("COPY $table TO STDOUT");
$dbh->rollback();
eval {
    $dbh->do(q{SELECT 'dbdpg_copytest'});
};
ok (!$@, $t);


#
# Keep old-style calls around for backwards compatibility
#

$t=q{old-style dbh->func('text', 'putline') still works};
$dbh->do("COPY $table FROM STDIN");
$result = $dbh->func("13\tOlive\n", 'putline');
is ($result, 1, $t);

$t=q{old-style dbh->func(var, length, 'getline') still works};
$dbh->pg_endcopy;
$dbh->do("COPY $table TO STDOUT");
$result = $dbh->func($data[0], 100, 'getline');
is ($result, 1, $t);
1 while ($result = $dbh->func($data[0], 100, 'getline'));

# Test binary copy mode
$dbh->do('CREATE TEMP TABLE binarycopy AS SELECT 1::INTEGER AS x');
$dbh->do('COPY binarycopy TO STDOUT BINARY');

my $copydata;
my $length = $dbh->pg_getcopydata($copydata);
while ($dbh->pg_getcopydata(my $tmp) >= 0) {
    $copydata .= $tmp;
}

ok (!utf8::is_utf8($copydata), 'pg_getcopydata clears UTF-8 flag on binary copy result');
is (substr($copydata, 0, 11), "PGCOPY\n\377\r\n\0", 'pg_getcopydata preserves binary copy header signature');
cmp_ok ($length, '>=', 19, 'pg_getcopydata returns sane length of binary copy');

$dbh->do('COPY binarycopy FROM STDIN BINARY');
eval {
    $dbh->pg_putcopydata($copydata);
    $dbh->pg_putcopyend;
};
is $@, '', 'pg_putcopydata in binary mode works'
    or diag $copydata;

$t=q{COPY in binary mode roundtrips};
is_deeply ($dbh->selectall_arrayref('SELECT * FROM binarycopy'), [[1],[1]], $t);

$dbh->do("DROP TABLE $table");
$dbh->commit();

cleanup_database($dbh,'test');
$dbh->disconnect;
