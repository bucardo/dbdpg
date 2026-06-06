#!perl

# Memory leak tests

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBI;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $procfile = '/proc/self/status';

if (! -r $procfile) {
    plan (skip_all =>  "Test skipped unless $procfile is available");
}

my $dbh = connect_database();
if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

plan tests => 2;

my $t = 'Connect to database for leak regression tests';

isnt ($dbh, undef, $t);

my $test_table = 'dbd_pg_leak_test';

$dbh->do(qq{
    CREATE TEMP TABLE $test_table (
        id   serial PRIMARY KEY,
        body text   NOT NULL
    )
});

## pg_switch_prepared=1 forces server-side prepare on the first execute,
## setting prepared_by_us=TRUE so DESTROY calls pg_st_deallocate_statement.
## Without this the default threshold (2) skips prepare on single-use sth.
$dbh->{pg_server_prepare}  = 1;
$dbh->{pg_switch_prepared} = 1;

sub _rss_kb {
    open my $fh, '<', $procfile or return 0;
    while (<$fh>) { return (split)[1] if /^VmRSS/; }
    return 0;
}

$dbh->do(qq{
        INSERT INTO $test_table (body)
        SELECT repeat('z', 512) FROM generate_series(1, 50)
    });

# Burn through one-time allocations before sampling RSS
for (1 .. 100) {
    my $sth = $dbh->prepare("SELECT body FROM $test_table WHERE id > ?");
    $sth->execute(0);
    1 while $sth->fetchrow_arrayref;
}

my $rss_before = _rss_kb();
for (1 .. 1000) {
    my $sth = $dbh->prepare("SELECT body FROM $test_table WHERE id > ?");
    $sth->execute(0);
    1 while $sth->fetchrow_arrayref;
}

my $rss_after = _rss_kb();
my $growth_kb = $rss_after - $rss_before;
my $limit_kb  = 8 * 1024;

$t = "RSS growth over 1000 cycles (${growth_kb} kB) is below ${limit_kb} kB limit";

ok ($growth_kb < $limit_kb, $t);

$dbh->rollback;
$dbh->disconnect;

