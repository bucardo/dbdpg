#!perl

## Regression tests for the PQclosePrepared result cleanup path (libpq >= 17).
## pg_st_deallocate_statement() used to overwrite imp_sth->result without
## first calling PQclear(), leaking one PGresult per prepare/execute/destroy.

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBI;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();
if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

my $pglibversion = $dbh->{pg_lib_version};

plan tests => 9;

isnt ($dbh, undef, 'Connect to database for PQclosePrepared leak regression tests');

## Setup

$dbh->{AutoCommit} = 0;

$dbh->do(q{
    CREATE TEMP TABLE dbd_pg_pqclose_leak_test (
        id   serial PRIMARY KEY,
        body text   NOT NULL
    )
});
$dbh->do(q{
    INSERT INTO dbd_pg_pqclose_leak_test (body)
    SELECT 'row' || n FROM generate_series(1, 50) AS n
});
$dbh->commit;

## pg_switch_prepared=1 forces server-side prepare on the first execute,
## setting prepared_by_us=TRUE so DESTROY calls pg_st_deallocate_statement.
## Without this the default threshold (2) skips prepare on single-use sth.
$dbh->{pg_server_prepare}  = 1;
$dbh->{pg_switch_prepared} = 1;

sub _rss_kb {
    open my $fh, '<', '/proc/self/status' or return 0;
    while (<$fh>) { return (split)[1] if /^VmRSS/; }
    return 0;
}

## Test 2: basic prepare/execute/fetch-all/destroy loop

{
    my $t = 'Repeated prepare/execute/fetch-all/destroy cycles complete without errors';
    my $ok = 1;
    eval {
        for (1 .. 20) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
            $sth->execute(0);
            1 while $sth->fetchrow_arrayref;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 3: fetchrow_array variant

{
    my $t = 'Repeated cycles using fetchrow_array complete without errors';
    my $ok = 1;
    eval {
        for (1 .. 20) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test ORDER BY id');
            $sth->execute;
            1 while $sth->fetchrow_array;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 4: finish() does not clear imp_sth->result; the leak path still fires
## when the sth is later destroyed.

{
    my $t = 'Cycles with finish() after partial fetch complete without errors';
    my $ok = 1;
    eval {
        for (1 .. 20) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
            $sth->execute(0);
            $sth->fetchrow_arrayref;
            $sth->finish;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 5: re-execute clears the previous result itself; the leak only fires
## on the final DESTROY.

{
    my $t = 'Multiple re-executes on one sth before destroy complete without errors';
    my $ok = 1;
    eval {
        my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
        for (1 .. 10) {
            $sth->execute(0);
            1 while $sth->fetchrow_arrayref;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 6: sth goes out of scope while still active (DBI calls finish then destroy).

{
    my $t = 'Destroying an active sth (partial fetch, no finish) does not crash';
    my $ok = 1;
    local $dbh->{Warn}      = 0;
    local $dbh->{PrintWarn} = 0;
    eval {
        for (1 .. 10) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test ORDER BY id');
            $sth->execute;
            $sth->fetchrow_arrayref;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 7: re-execute without draining rows; last active result must survive
## into DESTROY cleanly.

{
    my $t = 'Re-execute without full fetch each time, then destroy, does not crash';
    my $ok = 1;
    local $dbh->{Warn}      = 0;
    local $dbh->{PrintWarn} = 0;
    eval {
        my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
        for (1 .. 5) {
            $sth->execute(0);
            $sth->fetchrow_arrayref;
        }
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 8: after sth destroy, pg_error_field() on the dbh must still work
## (last_result handoff from sth to dbh must remain intact).

{
    my $t = 'pg_error_field is callable on $dbh after sth destroy';
    my $ok = 1;
    eval {
        {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
            $sth->execute(0);
            1 while $sth->fetchrow_arrayref;
        }
        $dbh->pg_error_field('state');
    };
    $ok = 0 if $@;
    ok ($ok, $t);
}

## Test 9: RSS growth guard, Linux + libpq >= 17 only.
## Without the fix each cycle leaks ~50 rows * 512 bytes; 1000 cycles gives
## visible growth well above the 8 MB limit used here.

SKIP: {
    skip 'Memory growth check requires Linux /proc', 1
        unless -r '/proc/self/status';

    skip 'PQclosePrepared path only compiled with libpq >= 17', 1
        if $pglibversion < 170000;

    $dbh->do(q{
        INSERT INTO dbd_pg_pqclose_leak_test (body)
        SELECT repeat('z', 512) FROM generate_series(1, 50)
    });
    $dbh->commit;

    my $rss_before = 0;

    my $ok = eval {
        # burn through one-time allocations before sampling RSS
        for (1 .. 100) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
            $sth->execute(0);
            1 while $sth->fetchrow_arrayref;
        }
        $rss_before = _rss_kb();
        for (1 .. 1000) {
            my $sth = $dbh->prepare('SELECT body FROM dbd_pg_pqclose_leak_test WHERE id > ?');
            $sth->execute(0);
            1 while $sth->fetchrow_arrayref;
        }
        1;
    };
    diag $@ if !$ok && $@;

    my $rss_after = _rss_kb();
    my $growth_kb = $rss_after - $rss_before;
    my $limit_kb  = 8 * 1024;

    ok ($ok && $growth_kb < $limit_kb,
        "RSS growth over 1000 cycles (${growth_kb} kB) is below ${limit_kb} kB limit"
    );
}

## Cleanup

$dbh->rollback;
$dbh->disconnect;
