#!perl

## Regression tests for async query ownership and data preservation (GitHub issue #105)

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBD::Pg ':async';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database({AutoCommit => 1});

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

## Use a second handle with RaiseError off for expected-failure tests
my $dbh_noerr = connect_database({AutoCommit => 1});
$dbh_noerr->{RaiseError} = 0;
$dbh_noerr->{PrintError} = 0;

plan tests => 57;

isnt ($dbh, undef, 'Connect to database for async regression testing');
isnt ($dbh_noerr, undef, 'Connect second handle with RaiseError off');

my ($sth, $sth1, $sth2, $sth3, $rows, $id, $val);

## pg_result() on the wrong statement handle returns an error and does not steal results

$sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
$sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC });

ok ($sth1->execute, 'Async execute on sth1 succeeds');

eval { $rows = $sth2->pg_result; };
ok ($@, 'pg_result() on wrong statement handle raises error');
like ($@, qr/wrong statement/i,
     'pg_result() on wrong statement handle mentions wrong statement');

$rows = $sth1->pg_result;
ok ($rows, 'pg_result() on correct statement handle succeeds after failed attempt by wrong handle');
is ($rows, 1, 'pg_result() on correct statement handle returns expected row count');

($id) = $sth1->fetchrow_array;
is ($id, 1, 'fetchrow_array on correct statement handle returns expected data');

$sth1->finish;
$sth2->finish;

## $dbh->pg_result() retrieves results and finished statement cannot steal new async results

$sth1 = $dbh->prepare(q{SELECT 1 AS num}, { pg_async => PG_ASYNC });
ok ($sth1->execute, 'Async execute on sth1 for dbh->pg_result test');

$rows = $dbh->pg_result;
ok ($rows, '$dbh->pg_result() succeeds for active async query');
is ($rows, 1, '$dbh->pg_result() returns expected row count');

($val) = $sth1->fetchrow_array;
is ($val, 1, 'Data is accessible via statement handle after $dbh->pg_result()');

eval { $dbh->pg_result; };
ok ($@, '$dbh->pg_result() with no pending async query raises error');
like ($@, qr/no async/i,
     '$dbh->pg_result() with no pending async query mentions no async');

$sth1->finish;

$sth2 = $dbh->prepare(q{SELECT 2}, { pg_async => PG_ASYNC });
ok ($sth2->execute, 'Async execute on sth2 after sth1 finished');

eval { $sth1->pg_result; };
ok ($@, 'Finished statement handle cannot retrieve results from new async query');

$sth2->pg_result;
$sth2->finish;

## Destroying an unrelated statement handle does not cancel an active async query

$sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
ok ($sth1->execute, 'Async execute on sth1 before destroying unrelated statement');

{
    $sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC });
    ok ($sth2, 'Unrelated statement handle created');
}
pass ('Unrelated statement handle destroyed via scope exit');

$rows = $sth1->pg_result;
ok ($rows, 'pg_result() succeeds after unrelated statement handle was destroyed');
is ($rows, 1, 'pg_result() returns expected row count after unrelated destroy');

($id) = $sth1->fetchrow_array;
is ($id, 1, 'fetchrow_array returns expected data after unrelated destroy');

ok ($sth1->finish, 'Statement finish succeeds after unrelated destroy');

## Only the statement that initiated the async query can retrieve its result

my @sths;
for my $i (1..3) {
    $sths[$i-1] = $dbh->prepare(qq{SELECT $i AS id}, { pg_async => PG_ASYNC });
    ok ($sths[$i-1], "Statement $i prepared for cross-statement test");
}

ok ($sths[1]->execute, 'Async execute on middle statement only');

eval { $sths[0]->pg_result; };
ok ($@, 'Statement that did not execute cannot retrieve async result');
like ($@, qr/no async|wrong statement/i,
     'Non-executing statement gets appropriate error');

eval { $sths[2]->pg_result; };
ok ($@, 'Other non-executing statement cannot retrieve async result');
like ($@, qr/no async|wrong statement/i,
     'Other non-executing statement gets appropriate error');

ok ($sths[1]->pg_result, 'Executing statement retrieves its own async result');
($id) = $sths[1]->fetchrow_array;
is ($id, 2, 'Executing statement gets correct data');

$_->finish for @sths;

## PG_OLDQUERY_WAIT auto-retrieves results for the owning statement

@sths = ();

for my $i (1..3) {
    $sths[$i-1] = $dbh->prepare(qq{SELECT $i AS id, pg_sleep(0.001)}, { pg_async => PG_ASYNC });
}

ok ($sths[0]->execute, 'Async execute on sth1 for interleaved OLDQUERY_WAIT test');

for my $i (2..3) {
    $sth = $dbh->prepare(qq{SELECT $i AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
    ok ($sth->execute, "Async execute with OLDQUERY_WAIT on sth$i");
    push @sths, $sth;
}

ok ($sths[3]->pg_result, 'pg_result() on last OLDQUERY_WAIT statement succeeds');
ok ($sths[0]->pg_result, 'pg_result() on first statement retrieves auto-stored results');
ok ($sths[4]->pg_result, 'pg_result() on middle OLDQUERY_WAIT statement succeeds');

($val) = $sths[0]->fetchrow_array;
is ($val, 1, 'First statement has correct auto-retrieved data');

($val) = $sths[3]->fetchrow_array;
is ($val, 2, 'Second OLDQUERY_WAIT statement has correct data');

($val) = $sths[4]->fetchrow_array;
is ($val, 3, 'Third OLDQUERY_WAIT statement has correct data');

$_->finish for grep { defined } @sths;
pass ('All interleaved statements finished cleanly');

## Errors from PG_OLDQUERY_WAIT are attributed to the correct statement
## Use the no-error handle since pg_result on error results raises

$dbh_noerr->do(q{DROP TABLE IF EXISTS async_test_constraints});
$dbh_noerr->do(q{CREATE TABLE async_test_constraints (id INT PRIMARY KEY)});
$dbh_noerr->do(q{INSERT INTO async_test_constraints VALUES (1)});

my $bad1 = $dbh_noerr->prepare(q{SELECT * FROM missing_table_1}, { pg_async => PG_ASYNC });
my $bad2 = $dbh_noerr->prepare(q{SELECT * FROM missing_table_2}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

ok ($bad1->execute, 'Async execute on query referencing missing_table_1');
ok ($bad2->execute, 'Async execute with OLDQUERY_WAIT on query referencing missing_table_2');

my $good = $dbh_noerr->prepare(q{SELECT 42}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
ok ($good->execute, 'Async execute with OLDQUERY_WAIT on valid query');

ok (!$bad1->pg_result, 'pg_result() on query with missing_table_1 fails');
like ($bad1->errstr || '', qr/missing_table_1/,
     'Error for missing_table_1 query mentions the correct table name');

ok (!$bad2->pg_result, 'pg_result() on query with missing_table_2 fails');
like ($bad2->errstr || '', qr/missing_table_2/,
     'Error for missing_table_2 query mentions the correct table name');

ok ($good->pg_result, 'pg_result() on valid query after error queries succeeds');

$bad1->finish;
$bad2->finish;
$good->finish;

$dbh_noerr->do(q{DROP TABLE async_test_constraints});

## PG_OLDQUERY_WAIT preserves data from the previous async query via auto-retrieve

$sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
ok ($sth1->execute, 'Async execute on sth1 for data preservation test');

$sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
ok ($sth2->execute, 'Async execute with OLDQUERY_WAIT triggers auto-retrieve of sth1 results');

($val) = $sth1->fetchrow_array;
is ($val, 1, 'Auto-retrieved data from sth1 is preserved and correct');

ok ($sth2->pg_result, 'pg_result() on sth2 after OLDQUERY_WAIT succeeds');
($val) = $sth2->fetchrow_array;
is ($val, 2, 'sth2 data is correct after OLDQUERY_WAIT');

ok ($sth2->finish, 'sth2 finish succeeds');
$sth1->finish;

## Clean up
$dbh->disconnect;
$dbh_noerr->disconnect;
