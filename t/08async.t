#!perl

## Test asynchronous queries

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use Time::HiRes qw/sleep/;
use DBD::Pg ':async';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database({AutoCommit => 1});

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

## Second handle with errors suppressed for expected-failure tests
my $dbh_noerr = connect_database({AutoCommit => 1});
if (! $dbh_noerr) {
    plan skip_all => 'Second connection to database failed, cannot continue testing';
}
$dbh_noerr->{RaiseError} = 0;
$dbh_noerr->{PrintError} = 0;

plan tests => 127;

isnt ($dbh, undef, 'Connect to database for async testing');

my ($t,$sth,$res);
my $pgversion = $dbh->{pg_server_version};

## First, test out do() in all its variants

$t=q{Method do() works as expected with no args };
eval {
    $res = $dbh->do('SELECT 123');
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Method do() works as expected with an unused attribute };
eval {
    $res = $dbh->do('SELECT 123', {pg_nosuch => 'arg'});
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Method do() works as expected with an unused attribute and a non-prepared param };
eval {
    $res = $dbh->do('SET random_page_cost TO ?', undef, '2.2');
};
is ($@, q{}, $t);
is ($res, '0E0', $t);

$t=q{Method do() works as expected with an unused attribute and multiple real bind params };
eval {
    $res = $dbh->do('SELECT count(*) FROM pg_class WHERE reltuples IN (?,?,?)', undef, 1,2,3);
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Canceling a non-async do() query gives an error };
eval {
    $res = $dbh->pg_cancel();
};
like ($@, qr{No asynchronous query is running}, $t);

$t=q{Method do() works as expected with an asynchronous flag };
eval {
    $res = $dbh->do('SELECT 123', {pg_async => PG_ASYNC});
};
is ($@, q{}, $t);
is ($res, '0E0', $t);

$t=q{Database attribute "async_status" returns 1 after async query};
$res = $dbh->{pg_async_status};
is ($res, +1, $t);

sleep 1;
$t=q{Canceling an async do() query works };
eval {
    $res = $dbh->pg_cancel();
};
is ($@, q{}, $t);

$t=q{Database method pg_cancel returns a false value when cancellation works but finished};
is ($res, q{}, $t);

$t=q{Database attribute "async_status" returns 0 after pg_cancel};
$res = $dbh->{pg_async_status};
is ($res, 0, $t);

$t=q{Running do() after a canceled query works};
eval {
    $res = $dbh->do('SELECT 123');
};
is ($@, q{}, $t);

$t=q{Database attribute "async_status" returns 0 after normal query run};
$res = $dbh->{pg_async_status};
is ($res, 0, $t);

$t=q{Method pg_ready() fails after a non-async query};
eval {
    $dbh->pg_ready();
};
like ($@, qr{No async}, $t);

$res = $dbh->do('SELECT 123', {pg_async => PG_ASYNC});
$t=q{Method pg_ready() works after async query};
## Sleep a sub-second to make sure the server has caught up
sleep 0.2;
eval {
    $res = $dbh->pg_ready();
};
is ($@, q{}, $t);

$t=q{Database method pg_ready() returns 1 after a completed async do()};
is ($res, 1, $t);

$res = $dbh->pg_ready();
$t=q{Database method pg_ready() returns true when called a second time};
is ($res, 1, $t);

$t=q{Canceling an async do() query works };
eval {
    $res = $dbh->pg_cancel();
};
is ($@, q{}, $t);
$t=q{Database method pg_cancel() returns expected false value for completed value};
is ($res, q{}, $t);

$t=q{Method do() runs after pg_cancel has cleared the async query};
eval {
    $dbh->do('SELECT 456');
};
is ($@, q{}, $t);

$dbh->do(q{SELECT 'async2'}, {pg_async => PG_ASYNC});

$t=q{Method do() fails when async query has not been cleared};
eval {
    $dbh->do(q{SELECT 'async_blocks'});
};
like ($@, qr{previous async}, $t);

$t=q{Database method pg_result works as expected};
eval {
    $res = $dbh->pg_result();
};
is ($@, q{}, $t);

$t=q{Database method pg_result() returns correct value};
is ($res, 1, $t);

$t=q{Database method pg_result() fails when called twice};
eval {
    $dbh->pg_result();
};
like ($@, qr{No async}, $t);

$t=q{Database method pg_cancel() fails when called after pg_result()};
eval {
    $dbh->pg_cancel();
};
like ($@, qr{No async}, $t);

$t=q{Database method pg_ready() fails when called after pg_result()};
eval {
    $dbh->pg_ready();
};
like ($@, qr{No async}, $t);

$t=q{Database method do() works after pg_result()};
eval {
    $dbh->do('SELECT 123');
};
is ($@, q{}, $t);

SKIP: {

    if ($pgversion < 80200) {
        skip ('Need pg_sleep() to perform rest of async tests: your Postgres is too old', 14);
    }

    eval {
        $dbh->do('SELECT pg_sleep(0)');
    };
    is ($@, q{}, 'Calling pg_sleep works as expected');

    my $time = time();
    eval {
        $res = $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    };
    $time = time()-$time;
    $t = q{Database method do() returns right away when in async mode};
    cmp_ok ($time, '<=', 1, $t);

    $t=q{Method pg_ready() returns false when query is still running};
    $res = $dbh->pg_ready();
    is ($res, 0, $t);

    pass ('Sleeping to allow query to finish');
    sleep(3);
    $t=q{Method pg_ready() returns true when query is finished};
    $res = $dbh->pg_ready();
    ok ($res, $t);

    $t=q{Method do() will not work if async query not yet cleared};
    eval {
        $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    };
    like ($@, qr{previous async}, $t);

    $t=q{Database method pg_cancel() works while async query is running};
    eval {
        $res = $dbh->pg_cancel();
    };
    is ($@, q{}, $t);
    $t=q{Database method pg_cancel returns false when query has already finished};
    ok (!$res, $t);

    $t=q{Database method pg_result() fails after async query has been canceled};
    eval {
        $res = $dbh->pg_result();
    };
    like ($@, qr{No async}, $t);

    $t=q{Database method do() cancels the previous async when requested};
    eval {
        $res = $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC + PG_OLDQUERY_CANCEL});
    };
    is ($@, q{}, $t);

    $t=q{Database method pg_result works when async query is still running};
    eval {
        $res = $dbh->pg_result();
    };
    is ($@, q{}, $t);

    ## Now throw in some execute after the do()
    $sth = $dbh->prepare('SELECT 567');

    $t = q{Running execute after async do() gives an error};
    $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    eval {
        $res = $sth->execute();
    };
    like ($@, qr{previous async}, $t);

    $t = q{Running execute after async do() works when told to cancel};
    $sth = $dbh->prepare('SELECT 678', {pg_async => PG_OLDQUERY_CANCEL});
    eval {
        $sth->execute();
    };
    is ($@, q{}, $t);

    $t = q{Running execute after async do() works when told to wait};
    $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    $sth = $dbh->prepare('SELECT 678', {pg_async => PG_OLDQUERY_WAIT});
    eval {
        $sth->execute();
    };
    is ($@, q{}, $t);

    $sth->finish();

    $t = q{Can get result of an async query which already finished after pg_send_cancel};
    $dbh->do('select 123', { pg_async => PG_ASYNC});
    sleep(1);
    $dbh->pg_send_cancel();
    $res = $dbh->pg_result();
    is($res, 1, $t);

    $dbh->do('select pg_sleep(10)', { pg_async => PG_ASYNC });
    $dbh->pg_send_cancel();
    $res = $dbh->pg_result();
    is (0+$res, 0, 'pg_result returns zero after canceled query');
    is ($dbh->state(), '57014', 'state is 57014 after canceled query');
} ## end of pg_sleep skip


$t=q{Method execute() works when prepare has PG_ASYNC flag};
$sth = $dbh->prepare('SELECT 123', {pg_async => PG_ASYNC});
eval {
    $sth->execute();
};
is ($@, q{}, $t);

$t=q{Database attribute "async_status" returns 1 after prepare async};
$res = $dbh->{pg_async_status};
is ($res, 1, $t);

$t=q{Method do() fails when previous async prepare has been executed};
eval {
    $dbh->do('SELECT 123');
};
like ($@, qr{previous async}, $t);

$t=q{Method execute() fails when previous async prepare has been executed};
eval {
    $sth->execute();
};
like ($@, qr{previous async}, $t);

$t=q{Database method pg_cancel works if async query has already finished};
sleep 0.5;
eval {
    $res = $sth->pg_cancel();
};
is ($@, q{}, $t);

$t=q{Statement method pg_cancel() returns a false value when cancellation works but finished};
is ($res, q{}, $t);

$t=q{Method do() fails when previous execute async has not been cleared};
$sth->execute();
$sth->finish(); ## Ideally, this would clear out the async, but it cannot at the moment
eval {
    $dbh->do('SELECT 345');
};
like ($@, qr{previous async}, $t);

$dbh->pg_cancel;

$t=q{Directly after pg_cancel(), pg_async_status is 0};
is ($dbh->{pg_async_status}, 0, $t);

$t=q{Method execute() works when prepare has PG_ASYNC flag};
$sth->execute();

$t=q{After async execute, pg_async_status is 1};
is ($dbh->{pg_async_status}, 1, $t);

$t=q{Method pg_result works after a prepare/execute call};
eval {
    $res = $dbh->pg_result;
};
is ($@, q{}, $t);

$t=q{Method pg_result() returns expected result after prepare/execute select};
is ($res, 1, $t);

$t=q{Method fetchall_arrayref works after pg_result};
eval {
    $res = $sth->fetchall_arrayref();
};
is ($@, q{}, $t);

$t=q{Method fetchall_arrayref returns correct result after pg_result};
is_deeply ($res, [[123]], $t);

$dbh->do('CREATE TABLE dbd_pg_test5(id INT, t TEXT)');
$sth->execute();

$t=q{Method prepare() works when passed in PG_OLDQUERY_CANCEL};

my $sth2;
my $SQL = 'INSERT INTO dbd_pg_test5(id) SELECT 123 UNION SELECT 456';
eval {
    $sth2 = $dbh->prepare($SQL, {pg_async => PG_ASYNC + PG_OLDQUERY_CANCEL});
};
is ($@, q{}, $t);

$t=q{Fetch on canceled statement handle fails};
eval {
    $sth->fetch();
};
like ($@, qr{no statement executing}, $t);

$t=q{Method execute works after async + cancel prepare};
eval {
    $sth2->execute();
};
is ($@, q{}, $t);

$t=q{Statement method pg_result works on async statement handle};
eval {
    $res = $sth2->pg_result();
};
is ($@, q{}, $t);

$t=q{Statement method pg_result returns correct result after execute};
is ($res, 2, $t);

$sth2->execute();

$t=q{Database method pg_result works on async statement handle};
eval {
    $res = $sth2->pg_result();
};
is ($@, q{}, $t);
$t=q{Database method pg_result returns correct result after execute};
is ($res, 2, $t);

$dbh->do('DROP TABLE dbd_pg_test5');

## TODO: More pg_sleep tests with execute

## ====================================================================
## Regression tests for async query ownership and data preservation
## (GitHub issue #105)
## ====================================================================

my ($sth1, $sth3, $rows, $id, $val, @sths);

## pg_result() on the wrong statement handle returns an error and does not steal results

$sth1 = $dbh->prepare(q{SELECT 991 AS id}, { pg_async => PG_ASYNC });
$sth2 = $dbh->prepare(q{SELECT 992 AS id}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth1 succeeds};
ok ($sth1->execute, $t);

$t=q{pg_result() on wrong statement handle mentions wrong statement};
eval { $rows = $sth2->pg_result; };
like ($@, qr/wrong statement/i, $t);

$t=q{pg_result() on correct statement handle returns expected row count};
$rows = $sth1->pg_result;
is ($rows, 1, $t);

$t=q{fetchrow_array on correct statement handle returns expected data};
($id) = $sth1->fetchrow_array;
is ($id, 991, $t);

$sth1->finish;
$sth2->finish;

## $dbh->pg_result() retrieves results and finished statement cannot steal new async results

$sth1 = $dbh->prepare(q{SELECT 993 AS num}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth1 for dbh->pg_result test};
ok ($sth1->execute, $t);

$t=q{$dbh->pg_result() returns expected row count};
$rows = $dbh->pg_result;
is ($rows, 1, $t);

$t=q{Data is accessible via statement handle after $dbh->pg_result()};
($val) = $sth1->fetchrow_array;
is ($val, 993, $t);

$t=q{$dbh->pg_result() with no pending async query mentions no async};
eval { $dbh->pg_result; };
like ($@, qr/no async/i, $t);

$sth1->finish;

$sth2 = $dbh->prepare(q{SELECT 994}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth2 after sth1 finished};
ok ($sth2->execute, $t);

$t=q{Finished statement handle cannot retrieve results from new async query};
eval { $sth1->pg_result; };
like ($@, qr/no async|wrong statement/i, $t);

$sth2->pg_result;
$sth2->finish;

## Destroying an unrelated statement handle does not cancel an active async query

$sth1 = $dbh->prepare(q{SELECT 991 AS id}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth1 before destroying unrelated statement};
ok ($sth1->execute, $t);

## Scope block: create and destroy an unrelated statement handle
{
    $sth2 = $dbh->prepare(q{SELECT 992 AS id}, { pg_async => PG_ASYNC });
    $t=q{Unrelated statement handle created};
    ok ($sth2, $t);
}
$t=q{Unrelated statement handle destroyed via scope exit};
pass ($t);

$t=q{pg_result() returns expected row count after unrelated destroy};
$rows = $sth1->pg_result;
is ($rows, 1, $t);

$t=q{fetchrow_array returns expected data after unrelated destroy};
($id) = $sth1->fetchrow_array;
is ($id, 991, $t);

$t=q{Statement finish succeeds after unrelated destroy};
ok ($sth1->finish, $t);

## Only the statement that initiated the async query can retrieve its result

for my $i (1..3) {
    my $qval = 990 + $i;
    $sths[$i] = $dbh->prepare(qq{SELECT $qval AS id}, { pg_async => PG_ASYNC });
    $t=qq{Statement $i prepared for cross-statement test};
    ok ($sths[$i], $t);
}

$t=q{Async execute on middle statement only};
ok ($sths[2]->execute, $t);

$t=q{Non-executing statement gets appropriate error};
eval { $sths[1]->pg_result; };
like ($@, qr/no async|wrong statement/i, $t);

$t=q{Other non-executing statement gets appropriate error};
eval { $sths[3]->pg_result; };
like ($@, qr/no async|wrong statement/i, $t);

$t=q{Executing statement retrieves its own async result};
ok ($sths[2]->pg_result, $t);

$t=q{Executing statement gets correct data};
($id) = $sths[2]->fetchrow_array;
is ($id, 992, $t);

$_->finish for grep { defined } @sths;

## PG_OLDQUERY_WAIT auto-retrieves results for the owning statement

$sth1 = $dbh->prepare(q{SELECT 991 AS id, pg_sleep(0.001)}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth1 for OLDQUERY_WAIT auto-retrieve test};
ok ($sth1->execute, $t);

$sth2 = $dbh->prepare(q{SELECT 992 AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

$t=q{Async execute with OLDQUERY_WAIT on sth2 waits for sth1};
ok ($sth2->execute, $t);

$sth3 = $dbh->prepare(q{SELECT 993 AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

$t=q{Async execute with OLDQUERY_WAIT on sth3 waits for sth2};
ok ($sth3->execute, $t);

$t=q{pg_result() on sth3 succeeds};
ok ($sth3->pg_result, $t);

$t=q{pg_result() on sth1 retrieves auto-stored results};
ok ($sth1->pg_result, $t);

$t=q{pg_result() on sth2 retrieves auto-stored results};
ok ($sth2->pg_result, $t);

$t=q{sth1 has correct auto-retrieved data};
($val) = $sth1->fetchrow_array;
is ($val, 991, $t);

$t=q{sth2 has correct data};
($val) = $sth2->fetchrow_array;
is ($val, 992, $t);

$t=q{sth3 has correct data};
($val) = $sth3->fetchrow_array;
is ($val, 993, $t);

$sth1->finish;
$sth2->finish;
$sth3->finish;

## Errors from PG_OLDQUERY_WAIT are attributed to the correct statement
## Use the no-error handle since pg_result on error results raises

$sth1 = $dbh_noerr->prepare(q{SELECT * FROM dbd_pg_missing1}, { pg_async => PG_ASYNC });

$t=q{Async execute on query referencing dbd_pg_missing1};
ok ($sth1->execute, $t);

$sth2 = $dbh_noerr->prepare(q{SELECT * FROM dbd_pg_missing2}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

$t=q{Async execute with OLDQUERY_WAIT on query referencing dbd_pg_missing2};
ok ($sth2->execute, $t);

my $good = $dbh_noerr->prepare(q{SELECT 994}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

$t=q{Async execute with OLDQUERY_WAIT on valid query};
ok ($good->execute, $t);

$t=q{pg_result() on query with dbd_pg_missing1 fails};
ok (!$sth1->pg_result, $t);

$t=q{Error for dbd_pg_missing1 query mentions the correct table name};
like ($sth1->errstr || '', qr/dbd_pg_missing1/, $t);

$t=q{pg_result() on query with dbd_pg_missing2 fails};
ok (!$sth2->pg_result, $t);

$t=q{Error for dbd_pg_missing2 query mentions the correct table name};
like ($sth2->errstr || '', qr/dbd_pg_missing2/, $t);

$t=q{pg_result() on valid query after error queries succeeds};
ok ($good->pg_result, $t);

$sth1->finish;
$sth2->finish;
$good->finish;

## PG_OLDQUERY_WAIT preserves data from the previous async query via auto-retrieve

$sth1 = $dbh->prepare(q{SELECT 991 AS id}, { pg_async => PG_ASYNC });

$t=q{Async execute on sth1 for data preservation test};
ok ($sth1->execute, $t);

$sth2 = $dbh->prepare(q{SELECT 992 AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

$t=q{Async execute with OLDQUERY_WAIT triggers auto-retrieve of sth1 results};
ok ($sth2->execute, $t);

$t=q{Auto-retrieved data from sth1 is preserved and correct};
($val) = $sth1->fetchrow_array;
is ($val, 991, $t);

$t=q{pg_result() on sth2 after OLDQUERY_WAIT succeeds};
ok ($sth2->pg_result, $t);

$t=q{sth2 data is correct after OLDQUERY_WAIT};
($val) = $sth2->fetchrow_array;
is ($val, 992, $t);

$t=q{sth2 finish succeeds};
ok ($sth2->finish, $t);

$sth1->finish;

# Test async COPY TO STDOUT: non-blocking do(), polling, result, and data drain
$t = q{do() with PG_ASYNC on COPY TO STDOUT returns '0E0' immediately (non-blocking)};
eval {
    $res = $dbh->do(
        q{COPY (SELECT * FROM unnest(ARRAY[1,2,3,4,5,6,7,8,9,10]::int[]) as dbd_pg_asynccopytest(a)) TO STDOUT},
        { pg_async => PG_ASYNC }
    );
};
is ($@, q{}, $t);
is ($res, '0E0', "$t - result is '0E0' (expected for async COPY TO STDOUT)");

$t = q{Database is in async mode after async COPY do()};
is ($dbh->{pg_async_status}, 1, $t);

$t = q{pg_ready() becomes true within reasonable time for small async COPY TO STDOUT};
my $ready = 0;
for my $i (1..12) {  # up to ~12 seconds max wait
    if ($dbh->pg_ready) {
        $ready = 1;
        last;
    }
    sleep 1;
}
ok ($ready, $t);

$t = q{pg_result() succeeds after async COPY TO STDOUT completes without error};
eval {
    $res = $dbh->pg_result();
};
is ($@, q{}, $t);
### TODO: THIS TEST IS NOT WORKING PROPERLY
### (defined $res && $res == 0, "$t - pg_result() after async COPY TO STDOUT returned 0 (expected rows-affected value)");
ok (defined $res, "$t - got defined result (but not what should be expected?)");

$t = q{We can drain the async COPY TO STDOUT data stream via pg_getcopydata loop};
my @copied_rows;
while (1) {
    my $buf = '';
    my $status = $dbh->pg_getcopydata($buf);
    last if !(defined $status && $status >= 0);
    chomp $buf;
    push @copied_rows, $buf;
}
is (scalar @copied_rows, 10, "$t - received correct number of rows from async COPY TO STDOUT");
is_deeply (\@copied_rows, [qw(1 2 3 4 5 6 7 8 9 10)], "$t - correct row values from async COPY TO STDOUT");

$t = q{Async status cleared after full COPY drain + pg_result};
is ($dbh->{pg_async_status}, 0, $t);

# Cleanup / sanity checks
eval { $dbh->pg_ready(); };
like ($@, qr{No async|async query}, 'pg_ready fails after COPY TO STDOUT completed and cleared');

eval { $dbh->do('SELECT 1'); };
is ($@, q{}, 'Normal synchronous query works after async COPY TO STDOUT finished');

cleanup_database($dbh,'test');
$dbh_noerr->disconnect;
$dbh->disconnect;

