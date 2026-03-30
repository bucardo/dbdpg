#!perl

## Test pipeline mode functionality (PostgreSQL 14+)

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use DBD::Pg ':async';
use Test::More;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

my $pgversion = $dbh->{pg_lib_version};

if ($pgversion < 140000) {
    plan skip_all => 'Pipeline mode requires PostgreSQL 14 or later';
}

plan tests => 49;

my ($result, $expected, $t);

$t='pg_pipeline_status returns 0 (off) by default';
my $status = $dbh->pg_pipeline_status();
is ($status, 0, $t);

# Enter pipeline mode

$t='pg_enter_pipeline_mode succeeds';
$result = $dbh->pg_enter_pipeline_mode();
is ($result, 1, $t);

$t='pg_pipeline_status returns 1 (on) after entering pipeline mode';
$status = $dbh->pg_pipeline_status();
is ($status, 1, $t);

$t='pg_enter_pipeline_mode is idempotent';
$result = $dbh->pg_enter_pipeline_mode();
is ($result, 1, $t);

# Exit pipeline mode

$t='pg_exit_pipeline_mode succeeds';
$result = $dbh->pg_exit_pipeline_mode();
is ($result, 1, $t);

$t='pg_pipeline_status returns 0 (off) after exiting pipeline mode';
$status = $dbh->pg_pipeline_status();
is ($status, 0, $t);

$t='pg_exit_pipeline_mode is idempotent when not in pipeline mode';
$result = $dbh->pg_exit_pipeline_mode();
is ($result, 1, $t);

$t='Normal queries work after entering and exiting pipeline mode';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

# pg_pipeline_sync

$t='pg_pipeline_sync fails when not in pipeline mode';
eval {
    $dbh->pg_pipeline_sync();
};
ok ($@, $t);

# pg_pipeline_sync + pg_getresult: basic sync cycle

$dbh->pg_enter_pipeline_mode();

$t='pg_pipeline_sync succeeds in pipeline mode';
$result = $dbh->pg_pipeline_sync();
is ($result, 1, $t);

$t='pg_getresult returns pipeline sync result';
$result = $dbh->pg_getresult();
is (ref $result, 'HASH', $t);

$t='Pipeline sync result has status PGRES_PIPELINE_SYNC';
# PGRES_PIPELINE_SYNC = 10 in libpq-fe.h
is ($result->{status}, 10, $t);

$t='pg_getresult returns undef after sync result';
my $null_result = $dbh->pg_getresult();
ok (!defined $null_result, $t);

$dbh->pg_exit_pipeline_mode();

# pg_getresult when not in pipeline mode

$t='pg_getresult returns undef when no results pending';
$null_result = $dbh->pg_getresult();
ok (!defined $null_result, $t);

# Verify connection is still good

$t='Connection works after sync cycle';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

$t='Enter/exit pipeline still works after sync cycle';
$dbh->pg_enter_pipeline_mode();
$dbh->pg_pipeline_sync();
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_getresult();  # NULL
$dbh->pg_exit_pipeline_mode();
eval { $dbh->do('SELECT 2'); };
is ($@, q{}, $t);

# pg_send_query_params: INSERT pipeline

$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline(id integer, name text)');

$t='pg_send_query_params queues an INSERT';
$dbh->pg_enter_pipeline_mode();
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)',
    [1, 'Alice']
);
is ($result, 1, $t);

$t='pg_send_query_params queues a second INSERT';
$result = $dbh->pg_send_query_params(
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)',
    [2, 'Bob']
);
is ($result, 1, $t);

$t='pg_pipeline_sync after INSERTs succeeds';
$result = $dbh->pg_pipeline_sync();
is ($result, 1, $t);

# Collect results: INSERT1 -> NULL -> INSERT2 -> NULL -> SYNC

$t='First INSERT result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 1, $t);  # PGRES_COMMAND_OK

$t='NULL separator after first INSERT';
my $null_result2 = $dbh->pg_getresult();
ok (!defined $null_result2, $t);

$t='Second INSERT result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 1, $t);

$t='NULL separator after second INSERT';
$null_result2 = $dbh->pg_getresult();
ok (!defined $null_result2, $t);

$t='PIPELINE_SYNC result';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == 10, $t);

$dbh->pg_exit_pipeline_mode();

$t='Pipeline INSERTs committed the data';
my $rows = $dbh->selectall_arrayref(
    'SELECT id, name FROM dbd_pg_test_pipeline ORDER BY id'
);
$expected = [[1, 'Alice'], [2, 'Bob']];
is_deeply ($rows, $expected, $t);

# pg_send_query_params: SELECT in pipeline

$dbh->pg_enter_pipeline_mode();

$t='pg_send_query_params queues a SELECT';
$result = $dbh->pg_send_query_params(
    'SELECT id, name FROM dbd_pg_test_pipeline WHERE id = $1',
    [1]
);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$t='SELECT result is TUPLES_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 2, $t);

$t='SELECT result has correct ntuples';
is ($result->{ntuples}, 1, $t);

$t='SELECT result has correct nfields';
is ($result->{nfields}, 2, $t);

$t='SELECT result rows contain correct data';
is_deeply ($result->{rows}, [['1', 'Alice']], $t);

$null_result = $dbh->pg_getresult();  # NULL separator
$result = $dbh->pg_getresult();       # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

# Pipeline error handling

$dbh->pg_enter_pipeline_mode();

$t='Send a query that will fail (syntax error)';
$result = $dbh->pg_send_query_params('GARBAGE SQL', []);
is ($result, 1, $t);

$t='Send a valid query after the bad one';
$result = $dbh->pg_send_query_params('SELECT 1 AS x', []);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

$t='First result is FATAL_ERROR';
$result = $dbh->pg_getresult();
is ($result->{status}, 7, $t);

$t='Error result has error message';
ok (defined $result->{error}, $t);

$null_result = $dbh->pg_getresult();  # NULL separator

$t='Second result is PIPELINE_ABORTED';
$result = $dbh->pg_getresult();
is ($result->{status}, 11, $t);

$null_result = $dbh->pg_getresult();  # NULL separator

$t='PIPELINE_SYNC after error recovery';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == 10, $t);

$dbh->pg_exit_pipeline_mode();

# Pipeline errors abort the implicit transaction, so we must rollback
$dbh->do('ROLLBACK');

$t='Connection works after pipeline error';
eval {
    $dbh->do('SELECT 1');
};
is ($@, q{}, $t);

# pg_send_prepare + pg_send_query_prepared
# The ROLLBACK above cleared the aborted transaction, which also rolled back
# the temp table and its data. Recreate and repopulate for the next tests.
$dbh->do('CREATE TEMP TABLE dbd_pg_test_pipeline(id integer, name text)');
$dbh->do(q{INSERT INTO dbd_pg_test_pipeline(id, name) VALUES (1, 'Alice'), (2, 'Bob')});

$dbh->pg_enter_pipeline_mode();

$t='pg_send_prepare queues a PREPARE';
$result = $dbh->pg_send_prepare(
    'pipeline_insert',
    'INSERT INTO dbd_pg_test_pipeline(id, name) VALUES ($1, $2)'
);
is ($result, 1, $t);

$t='pg_send_query_prepared queues execution';
$result = $dbh->pg_send_query_prepared('pipeline_insert', [3, 'Charlie']);
is ($result, 1, $t);

$t='pg_send_query_prepared queues second execution';
$result = $dbh->pg_send_query_prepared('pipeline_insert', [4, 'Diana']);
is ($result, 1, $t);

$dbh->pg_pipeline_sync();

# Collect: PREPARE -> NULL -> INSERT1 -> NULL -> INSERT2 -> NULL -> SYNC
$t='PREPARE result is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 1, $t);
$dbh->pg_getresult();  # NULL

$t='First prepared INSERT is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 1, $t);
$dbh->pg_getresult();  # NULL

$t='Second prepared INSERT is COMMAND_OK';
$result = $dbh->pg_getresult();
is ($result->{status}, 1, $t);
$dbh->pg_getresult();  # NULL

$t='PIPELINE_SYNC after prepared statements';
$result = $dbh->pg_getresult();
ok (defined $result && $result->{status} == 10, $t);

$dbh->pg_exit_pipeline_mode();

$t='Prepared statement data committed';
$rows = $dbh->selectall_arrayref(
    'SELECT id, name FROM dbd_pg_test_pipeline ORDER BY id'
);
$expected = [[1, 'Alice'], [2, 'Bob'], [3, 'Charlie'], [4, 'Diana']];
is_deeply ($rows, $expected, $t);

# Deallocate the prepared statement
$dbh->do('DEALLOCATE pipeline_insert');

# pg_send_flush_request

$t='pg_send_flush_request fails outside pipeline mode';
eval {
    $dbh->pg_send_flush_request();
};
ok ($@, $t);

$dbh->pg_enter_pipeline_mode();

$t='pg_send_flush_request succeeds in pipeline mode';
$dbh->pg_send_query_params('SELECT 42 AS answer', []);
$result = $dbh->pg_send_flush_request();
is ($result, 1, $t);

# Flush the output buffer and collect result
$dbh->pg_flush();

$t='Flush request lets us retrieve result without sync';
$result = $dbh->pg_getresult();
is ($result->{status}, 2, $t);
is_deeply ($result->{rows}, [['42']], "$t data correct");

$dbh->pg_getresult();  # NULL separator

# Need a sync to cleanly exit
$dbh->pg_pipeline_sync();
$dbh->pg_getresult();  # PIPELINE_SYNC
$dbh->pg_exit_pipeline_mode();

cleanup_database($dbh,'test');
$dbh->disconnect;
