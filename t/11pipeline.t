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

plan tests => 8;

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

cleanup_database($dbh,'test');
$dbh->disconnect;
