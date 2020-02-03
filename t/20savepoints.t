#!perl

## Test savepoint functionality

use 5.008001;
use strict;
use warnings;
use Test::More;
use DBI ':sql_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 3;

isnt ($dbh, undef, 'Connect to database for savepoint testing');

my $t;

my $str = 'Savepoint Test';
my $sth = $dbh->prepare('INSERT INTO dbd_pg_test (id,pname) VALUES (?,?)');

## Create 500 without a savepoint
$sth->execute(500,$str);

## Create 501 inside a savepoint and roll it back
$dbh->pg_savepoint('dbd_pg_test_savepoint');
$sth->execute(501,$str);

$dbh->pg_rollback_to('dbd_pg_test_savepoint');
$dbh->pg_rollback_to('dbd_pg_test_savepoint'); ## Yes, we call it twice

## Create 502 after the rollback:
$sth->execute(502,$str);

$dbh->commit;

$t='Only row 500 and 502 should be committed';
my $ids = $dbh->selectcol_arrayref('SELECT id FROM dbd_pg_test WHERE pname = ?',undef,$str);
ok (eq_set($ids, [500, 502]), $t);

## Create 503, then release the savepoint
$dbh->pg_savepoint('dbd_pg_test_savepoint');
$sth->execute(503,$str);
$dbh->pg_release('dbd_pg_test_savepoint');

## Create 504 outside of any savepoint
$sth->execute(504,$str);
$dbh->commit;

$t='Implicit rollback on deallocate should rollback to last savepoint';
$ids = $dbh->selectcol_arrayref('SELECT id FROM dbd_pg_test WHERE pname = ?',undef,$str);
ok (eq_set($ids, [500, 502, 503, 504]), $t);

$dbh->do('DELETE FROM dbd_pg_test');
$dbh->commit();

cleanup_database($dbh,'test');
$dbh->disconnect();
