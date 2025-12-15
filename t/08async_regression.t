#!/usr/bin/env perl
#
# Regression tests for async issues fixed in DBD::Pg
# These tests validate the fixes for GitHub issue #105 and related problems
#
# See async-fix-105.patch for the implementation details
# See issue-105.md for comprehensive documentation
#

use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBI;
use DBD::Pg qw(:async);

my $dsn = $ENV{DBI_DSN} || "dbi:Pg:dbname=postgres;host=localhost";
my $user = $ENV{PGUSER} || $ENV{DBI_USER} || 'postgres';
my $pass = $ENV{PGPASSWORD} || $ENV{DBI_PASS} || '';

my $dbh = DBI->connect($dsn, $user, $pass, {
    AutoCommit => 1,
    RaiseError => 0,
    PrintError => 0,
});

unless ($dbh) {
    plan skip_all => "Cannot connect to PostgreSQL: $DBI::errstr";
}

# Test 1: Basic ownership - wrong statement cannot steal results
#
# ISSUE: Without the fix, any statement handle could call pg_result() and
# retrieve another statement's async results, violating ownership semantics.
#
# FIX (async-fix-105.patch lines 133-139): Added ownership check in pg_db_result()
# to verify imp_sth == imp_dbh->async_sth before allowing result retrieval.
#
# BEHAVIOR:
# - BEFORE: sth2->pg_result() would successfully steal sth1's pending results
# - AFTER: sth2->pg_result() fails with "wrong statement handle" error
subtest 'Basic ownership - wrong statement' => sub {
    plan tests => 6;

    my $sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
    my $sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC });

    ok($sth1->execute, 'sth1 executes');

    # Try to steal results with wrong statement
    my $stolen = $sth2->pg_result;
    ok(!$stolen, 'sth2 cannot steal sth1 results');
    like($sth2->errstr || '', qr/wrong statement|not.*owner/i,
         'Error indicates wrong statement');

    # Original statement should still work
    my $rows = $sth1->pg_result;
    ok($rows, 'sth1 can still get its results');
    is($rows, 1, 'Correct row count');

    my ($id) = $sth1->fetchrow_array;
    is($id, 1, 'Correct data retrieved');

    $sth1->finish;
    $sth2->finish;
};

# Test 2: $dbh->pg_result works with ownership verification
#
# ISSUE: After a statement was finished, it could still retrieve results
# from new async queries started by other statements.
#
# FIX (async-fix-105.patch lines 46-53, 133-139):
# - Added statement type detection to distinguish sth from dbh calls
# - Ownership check prevents finished statements from stealing new results
#
# BEHAVIOR:
# - BEFORE: Finished sth1 could retrieve sth2's async results
# - AFTER: Finished sth1 gets "wrong statement handle" error
subtest '$dbh->pg_result ownership' => sub {
    plan tests => 8;

    my $sth1 = $dbh->prepare(q{SELECT 1 AS num}, { pg_async => PG_ASYNC });
    ok($sth1->execute, 'Statement executes');

    # $dbh->pg_result should work (database handle can retrieve any result)
    my $rows = $dbh->pg_result;
    ok($rows, '$dbh->pg_result succeeds');
    is($rows, 1, 'Correct row count from $dbh');

    # Data should be accessible via statement
    my ($num) = $sth1->fetchrow_array;
    is($num, 1, 'Data accessible via statement');

    # No async pending
    ok(!$dbh->pg_result, '$dbh->pg_result with no async fails');
    like($dbh->errstr || '', qr/no async/i, 'Error mentions no async');

    $sth1->finish;

    # After finish, new async by different statement
    my $sth2 = $dbh->prepare(q{SELECT 2}, { pg_async => PG_ASYNC });
    ok($sth2->execute, 'sth2 executes');

    # Finished statement shouldn't retrieve new async
    ok(!$sth1->pg_result, 'Finished statement cannot retrieve new async');

    $sth2->pg_result;
    $sth2->finish;
};

# Test 3: Statement ownership after destroy
#
# ISSUE #105: Destroying an unrelated statement handle would cancel
# the active async query of another statement, even if the destroyed
# statement never executed any async query.
#
# ROOT CAUSE: dbd_st_destroy() unconditionally called handle_old_async()
# and cleared imp_dbh->async_sth whenever ANY statement was destroyed.
#
# FIX (async-fix-105.patch lines 23-28, 34-38):
# - dbd_st_destroy() only calls handle_old_async() if imp_dbh->async_sth == imp_sth
# - Only clears imp_dbh->async_sth if this statement owns it
#
# BEHAVIOR:
# - BEFORE: Destroying sth2 would cancel sth1's pending async query
# - AFTER: Destroying sth2 has no effect on sth1's async query
subtest 'Statement ownership after destroy' => sub {
    plan tests => 7;

    # Create and execute first statement
    my $sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
    ok($sth1->execute, 'sth1 executes');

    # Create second statement, destroy without executing
    {
        my $sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC });
        ok($sth2, 'sth2 created');
        # Let it go out of scope - this triggers dbd_st_destroy()
    }
    pass('sth2 destroyed');

    # sth1 should still work (its async query wasn't cancelled)
    my $rows = $sth1->pg_result;
    ok($rows, 'sth1 can still retrieve');
    is($rows, 1, 'Correct row count');

    my ($id) = $sth1->fetchrow_array;
    is($id, 1, 'Correct data');

    ok($sth1->finish, 'sth1 finishes');
};

# Test 4: Cross-statement interference prevention
#
# ISSUE: Multiple prepared statements could interfere with each other's
# async operations due to lack of ownership tracking.
#
# FIX (async-fix-105.patch lines 10-15, 133-139):
# - dbd_st_finish() only clears async_sth if imp_dbh->async_sth == imp_sth
# - pg_db_result() verifies ownership before retrieving
#
# BEHAVIOR:
# - BEFORE: Any statement could retrieve any async result
# - AFTER: Only the statement that initiated the async query can retrieve it
subtest 'Cross-statement interference' => sub {
    plan tests => 10;

    # Multiple statements prepared but only one executed
    my @statements;
    for my $i (1..3) {
        $statements[$i-1] = $dbh->prepare(qq{SELECT $i AS id}, { pg_async => PG_ASYNC });
        ok($statements[$i-1], "Statement $i prepared");
    }

    # Execute only the middle one
    ok($statements[1]->execute, 'Statement 2 executes');

    # Others shouldn't be able to retrieve
    ok(!$statements[0]->pg_result, 'Statement 1 cannot retrieve');
    like($statements[0]->errstr || '', qr/no async|wrong statement/i,
         'Statement 1 error correct');

    ok(!$statements[2]->pg_result, 'Statement 3 cannot retrieve');
    like($statements[2]->errstr || '', qr/no async|wrong statement/i,
         'Statement 3 error correct');

    # Only statement 2 should retrieve
    ok($statements[1]->pg_result, 'Statement 2 retrieves');
    my ($id) = $statements[1]->fetchrow_array;
    is($id, 2, 'Statement 2 data correct');

    $_->finish for @statements;
};

# Test 5: Interleaved operations with OLDQUERY_WAIT
#
# ISSUE: PG_OLDQUERY_WAIT would discard the previous async query's results
# instead of preserving them for later retrieval by the owning statement.
#
# FIX (async-fix-105.patch - handle_old_async enhancement at line 5715+):
# - When PG_OLDQUERY_WAIT is used, results are auto-retrieved and stored
# - The owning statement's async_status is set to 100 (auto-retrieved)
# - Results are preserved in imp_sth->result for later access
#
# BEHAVIOR:
# - BEFORE: sth1's results would be lost when sth2 uses OLDQUERY_WAIT
# - AFTER: sth1's results are auto-retrieved and accessible via fetchrow
subtest 'Interleaved operations with auto-retrieve' => sub {
    plan tests => 10;

    my @sths;

    # Create multiple statements
    for my $i (1..3) {
        $sths[$i-1] = $dbh->prepare(qq{SELECT $i AS id, pg_sleep(0.001)}, { pg_async => PG_ASYNC });
    }

    # Execute first
    ok($sths[0]->execute, 'sth1 executes');

    # Try others with OLDQUERY_WAIT - this triggers auto-retrieve of sth1's results
    for my $i (2..3) {
        my $sth = $dbh->prepare(qq{SELECT $i AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
        ok($sth->execute, "sth$i with WAIT executes");
        push @sths, $sth;
    }

    # Retrieve in different order
    ok($sths[3]->pg_result, 'sth4 retrieves');
    ok($sths[0]->pg_result, 'sth1 retrieves (auto-stored)');
    ok($sths[4]->pg_result, 'sth5 retrieves');

    # Verify data - all should be preserved correctly
    my ($id1) = $sths[0]->fetchrow_array;
    is($id1, 1, 'sth1 has correct data');

    my ($id2) = $sths[3]->fetchrow_array;
    is($id2, 2, 'sth4 has correct data');

    my ($id3) = $sths[4]->fetchrow_array;
    is($id3, 3, 'sth5 has correct data');

    # Clean up
    $_->finish for grep { defined } @sths;
    pass('All statements finished');
};

# Test 6: Error attribution with OLDQUERY_WAIT
#
# ISSUE: When multiple async queries with errors were executed using
# OLDQUERY_WAIT, errors would not be properly attributed to the correct
# statement that caused them.
#
# FIX (async-fix-105.patch lines 113-131):
# - Auto-retrieved error results are stored with async_status = -1
# - Each statement maintains its own error state and message
# - Errors are properly reported when pg_result() is called
#
# BEHAVIOR:
# - BEFORE: Errors could be misattributed or lost entirely
# - AFTER: Each statement reports its own specific error
subtest 'Error attribution with OLDQUERY_WAIT' => sub {
    plan tests => 8;

    # Create temporary table
    $dbh->do(q{DROP TABLE IF EXISTS async_test_constraints});
    $dbh->do(q{CREATE TABLE async_test_constraints (id INT PRIMARY KEY)});
    $dbh->do(q{INSERT INTO async_test_constraints VALUES (1)});

    # Start two async queries that will fail with different errors
    my $bad1 = $dbh->prepare(q{SELECT * FROM missing_table_1}, { pg_async => PG_ASYNC });
    my $bad2 = $dbh->prepare(q{SELECT * FROM missing_table_2}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });

    ok($bad1->execute, 'bad1 executes');
    ok($bad2->execute, 'bad2 executes with OLDQUERY_WAIT');

    # Also start a good query
    my $good = $dbh->prepare(q{SELECT 42}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
    ok($good->execute, 'good executes');

    # Errors should be attributed correctly to each statement
    ok(!$bad1->pg_result, 'bad1 pg_result fails');
    like($bad1->errstr || '', qr/missing_table_1/, 'bad1 error mentions correct table');

    ok(!$bad2->pg_result, 'bad2 pg_result fails');
    like($bad2->errstr || '', qr/missing_table_2/, 'bad2 error mentions correct table');

    ok($good->pg_result, 'good pg_result succeeds');

    $bad1->finish;
    $bad2->finish;
    $good->finish;

    # Clean up
    $dbh->do(q{DROP TABLE async_test_constraints});
};

# Test 7: OLDQUERY_WAIT auto-retrieve preserves data
#
# ISSUE: PG_OLDQUERY_WAIT would wait for the previous query to complete
# but would discard its results, causing data loss.
#
# FIX (async-fix-105.patch lines 54-64, 81-112):
# - Results are auto-retrieved and stored in the owning statement
# - async_status = 100 indicates auto-retrieved results are available
# - pg_result() returns the stored row count without re-fetching
# - fetchrow_array() accesses the preserved result data
#
# BEHAVIOR:
# - BEFORE: sth1's data would be lost, fetchrow_array would return undef
# - AFTER: sth1's data is preserved and accessible
subtest 'OLDQUERY_WAIT auto-retrieve preserves data' => sub {
    plan tests => 6;

    my $sth1 = $dbh->prepare(q{SELECT 1 AS id}, { pg_async => PG_ASYNC });
    ok($sth1->execute, 'sth1 executes');

    # Execute another with OLDQUERY_WAIT - should auto-retrieve sth1's results
    my $sth2 = $dbh->prepare(q{SELECT 2 AS id}, { pg_async => PG_ASYNC + PG_OLDQUERY_WAIT });
    ok($sth2->execute, 'sth2 executes with WAIT');

    # sth1's results should have been auto-retrieved and preserved
    # This is the critical test - data must not be lost
    my ($val1) = $sth1->fetchrow_array;
    is($val1, 1, 'sth1 data auto-retrieved correctly');

    # sth2 should work normally
    ok($sth2->pg_result, 'sth2 pg_result works');
    my ($val2) = $sth2->fetchrow_array;
    is($val2, 2, 'sth2 data correct');

    ok($sth2->finish, 'sth2 finishes');

    $sth1->finish;
};

# Clean up
$dbh->disconnect;
done_testing();
