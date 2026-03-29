#!perl

## Test the COPY functionality

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Data::Dumper;
use DBD::Pg ':async';
use Test::More;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if ($dbh) {
    plan tests => 89;
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
my $buffer = '';
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

$t='getline returned all rows successfully';
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
$expected = "PGCOPY\n\377\r\n\0";
is (substr($copydata, 0, 11), $expected, 'pg_getcopydata preserves binary copy header signature');
cmp_ok ($length, '>=', 19, 'pg_getcopydata returns sane length of binary copy');

$dbh->do('COPY binarycopy FROM STDIN BINARY');
eval {
    $dbh->pg_putcopydata($copydata);
    $dbh->pg_putcopyend;
};
is $@, '', 'pg_putcopydata in binary mode works'
    or diag $copydata;

$t=q{COPY in binary mode round trips};
is_deeply ($dbh->selectall_arrayref('SELECT * FROM binarycopy'), [[1],[1]], $t); ## nospellcheck

##
## Test the async COPY methods
##

my $async_table = 'dbd_pg_test_async_copy';
$dbh->do(qq{CREATE TABLE $async_table(id integer, name text)});
$dbh->commit();

# pg_putcopydata_async: basic operation

$t='pg_putcopydata_async fails if not after a COPY FROM statement';
eval {
    $dbh->pg_putcopydata_async("pizza\tpie");
};
like ($@, qr{COPY FROM command}, $t);

$t='pg_putcopydata_async returns 1 on success';
$dbh->do("COPY $async_table FROM STDIN");
$result = $dbh->pg_putcopydata_async("1\tAlice\n");
is ($result, 1, $t);

$t='pg_flush sends data to server';
$result = $dbh->pg_flush();
is ($result, 0, $t); # 0 = flushed, 1 = pending

$t='pg_putcopydata_async works on second call';
$result = $dbh->pg_putcopydata_async("2\tBob\n");
is ($result, 1, $t);
$dbh->pg_flush();

$t='pg_putcopydata_async works on third call';
$result = $dbh->pg_putcopydata_async("3\tCharlie\n");
is ($result, 1, $t);
$dbh->pg_flush();

# pg_putcopyend_async: basic operation

$t='pg_putcopyend_async completes the COPY';
my $end_result = $dbh->pg_putcopyend_async();
# May need to poll if result is 0 (not ready yet)
my $poll_count = 0;
while ($end_result == 0 && $poll_count < 100) {
    select(undef, undef, undef, 0.01);
    $end_result = $dbh->pg_putcopyend_async();
    $poll_count++;
}
is ($end_result, 1, $t);

$t='Data from pg_putcopydata_async was inserted correctly';
$result = $dbh->selectall_arrayref("SELECT id,name FROM $async_table ORDER BY id");
$expected = [[1,'Alice'],[2,'Bob'],[3,'Charlie']];
is_deeply ($result, $expected, $t);

$dbh->commit();

# Normal queries work after async COPY

$t='Normal queries work after async COPY IN';
eval {
    $dbh->do('SELECT 999');
};
is ($@, q{}, $t);

# Async queries work after async COPY

$t='Async queries work after async COPY IN';
eval {
    $dbh->do('SELECT 888', { pg_async => PG_ASYNC} );
};
is ($@, q{}, $t);
$dbh->pg_result();

# pg_putcopyend_async: state checks (uses Test::Warn like blocking variant)

$t='pg_putcopyend_async warns when not in COPY state';
eval { require Test::Warn; };
if ($@) {
    pass ('Skipping Test::Warn test for putcopyend_async no-copy');
    pass ('Skipping Test::Warn test for putcopyend_async copy-out');
}
else {
    Test::Warn::warning_like (sub { $dbh->pg_putcopyend_async(); }, qr/until a COPY/, $t);

    $t='pg_putcopyend_async warns when in COPY OUT state';
    $dbh->do("COPY $async_table TO STDOUT");
    Test::Warn::warning_like (sub { $dbh->pg_putcopyend_async(); }, qr/pg_getcopydata/, $t);
    # Drain the COPY OUT
    1 while ($dbh->pg_getcopydata($buffer) >= 0);
}

# pg_flush: works outside COPY (should just return 0 = nothing to flush)

$t='pg_flush returns 0 when nothing to flush';
$result = $dbh->pg_flush();
is ($result, 0, $t);

# Async COPY with larger data set (tests buffering behavior)

$dbh->do("DELETE FROM $async_table");
$dbh->commit();

$t='pg_putcopydata_async handles larger data sets';
$dbh->do("COPY $async_table FROM STDIN");
my $async_ok = 1;
for my $i (1..1000) {
    my $row_result = $dbh->pg_putcopydata_async("$i\tRow number $i\n");
    if ($row_result == -1) {
        $async_ok = 0;
        last;
    }
    # If buffer full (0), poll and retry
    while ($row_result == 0) {
        select(undef, undef, undef, 0.001);
        $row_result = $dbh->pg_putcopydata_async("$i\tRow number $i\n");
    }
    # Flush after each successful queue
    my $flush = $dbh->pg_flush();
    while ($flush == 1) {
        select(undef, undef, undef, 0.001);
        $flush = $dbh->pg_flush();
    }
}
ok ($async_ok, $t);

$t='pg_putcopyend_async works after large data set';
$end_result = $dbh->pg_putcopyend_async();
$poll_count = 0;
while ($end_result == 0 && $poll_count < 100) {
    select(undef, undef, undef, 0.01);
    $end_result = $dbh->pg_putcopyend_async();
    $poll_count++;
}
is ($end_result, 1, $t);

$t='All 1000 rows were inserted via async COPY';
$result = $dbh->selectall_arrayref("SELECT count(*) FROM $async_table");
is ($result->[0][0], 1000, $t);

$dbh->commit();

# Mixing: blocking putcopydata still works (backward compatibility)

$dbh->do("DELETE FROM $async_table");
$dbh->commit();

$t='Blocking pg_putcopydata still works after async has been used';
$dbh->do("COPY $async_table FROM STDIN");
$result = $dbh->pg_putcopydata("42\tBlocking row\n");
is ($result, 1, $t);

$t='Blocking pg_putcopyend still works';
$result = $dbh->pg_putcopyend();
is ($result, 1, $t);

$t='Blocking COPY data was inserted correctly';
$result = $dbh->selectall_arrayref("SELECT id,name FROM $async_table ORDER BY id");
$expected = [[42,'Blocking row']];
is_deeply ($result, $expected, $t);

$dbh->commit();

# pg_putcopydata_async: wrong state checks

$t='pg_putcopydata_async fails in COPY OUT state';
$dbh->do("COPY $async_table TO STDOUT");
eval {
    $dbh->pg_putcopydata_async("pizza\tpie");
};
like ($@, qr{COPY FROM command}, $t);
# Drain the COPY OUT
1 while ($dbh->pg_getcopydata($buffer) >= 0);

$t='pg_putcopydata_async fails with no argument';
$dbh->do("COPY $async_table FROM STDIN");
eval {
    $dbh->pg_putcopydata_async();
};
ok ($@, $t);
$dbh->rollback();

# do() fails during async COPY IN (same as blocking)

$t='do() fails during async COPY IN';
$dbh->do("COPY $async_table FROM STDIN");
$dbh->pg_putcopydata_async("99\tDuringCopy\n");
eval {
    $dbh->do('SELECT 123');
};
like ($@, qr{pg_putcopyend}, $t);

$t='pg_putcopydata_async works after a rude non-COPY attempt';
eval {
    $result = $dbh->pg_putcopydata_async("100\tAfterRude\n");
};
is ($@, q{}, $t);
is ($result, 1, $t);
$dbh->pg_flush();
$dbh->pg_putcopyend();
$dbh->commit();

# Binary COPY with async methods

$dbh->do('CREATE TEMP TABLE dbd_pg_test_binarycopy_async AS SELECT 1::INTEGER AS x');
$dbh->do('COPY dbd_pg_test_binarycopy_async TO STDOUT BINARY');

my $bindata;
my $binlen = $dbh->pg_getcopydata($bindata);
while ($dbh->pg_getcopydata(my $tmp) >= 0) {
    $bindata .= $tmp;
}

$t='pg_putcopydata_async works in binary mode';
$dbh->do('COPY dbd_pg_test_binarycopy_async FROM STDIN BINARY');
eval {
    $dbh->pg_putcopydata_async($bindata);
    $dbh->pg_flush();
    my $bend = $dbh->pg_putcopyend_async();
    my $bpoll = 0;
    while ($bend == 0 && $bpoll < 100) {
        select(undef, undef, undef, 0.01);
        $bend = $dbh->pg_putcopyend_async();
        $bpoll++;
    }
};
is ($@, '', $t);

$t='Binary COPY via async round trips correctly';
is_deeply ($dbh->selectall_arrayref('SELECT * FROM dbd_pg_test_binarycopy_async'), [[1],[1]], $t); ## nospellcheck

# Multiple async COPY cycles on the same connection

$dbh->do("DELETE FROM $async_table");
$dbh->commit();

$t='Second async COPY cycle works on same connection';
$dbh->do("COPY $async_table FROM STDIN");
$dbh->pg_putcopydata_async("50\tFirstCycle\n");
$dbh->pg_flush();
my $e1 = $dbh->pg_putcopyend_async();
while ($e1 == 0) { select(undef, undef, undef, 0.01); $e1 = $dbh->pg_putcopyend_async(); }
$dbh->commit();

$dbh->do("COPY $async_table FROM STDIN");
$dbh->pg_putcopydata_async("51\tSecondCycle\n");
$dbh->pg_flush();
my $e2 = $dbh->pg_putcopyend_async();
while ($e2 == 0) { select(undef, undef, undef, 0.01); $e2 = $dbh->pg_putcopyend_async(); }
is ($e2, 1, $t);

$t='Both async COPY cycles inserted data correctly';
$result = $dbh->selectall_arrayref("SELECT id,name FROM $async_table ORDER BY id");
$expected = [[50,'FirstCycle'],[51,'SecondCycle']];
is_deeply ($result, $expected, $t);

$dbh->commit();

$dbh->do("DROP TABLE $table");
$dbh->commit();

cleanup_database($dbh,'test');
$dbh->disconnect;
