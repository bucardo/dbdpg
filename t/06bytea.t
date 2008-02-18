#!perl

## Test bytea handling

use strict;
use warnings;
use Test::More;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 7;

isnt( $dbh, undef, 'Connect to database for bytea testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

my $sth;

$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest) VALUES (?,?)});

$sth->bind_param(2, undef, { pg_type => PG_BYTEA });
ok( $sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), 'bytea insert test with string containing null and backslashes');
ok( $sth->execute(401, '\''), 'bytea insert test with string containing a single quote');
ok( $sth->execute(402, '\''), 'bytea (second) insert test with string containing a single quote');

$sth = $dbh->prepare(q{SELECT bytetest FROM dbd_pg_test WHERE id=?});

$sth->execute(400);
my $byte = $sth->fetchall_arrayref()->[0][0];
is( $byte, 'aa\bb\cc\\\0dd\\', 'Received correct text from BYTEA column with backslashes');

$sth->execute(402);
$byte = $sth->fetchall_arrayref()->[0][0];
is( $byte, '\'', 'Received correct text from BYTEA column with quote');

my $string = "abc\123\\def\0ghi";
my $result = $dbh->quote($string, { pg_type => PG_BYTEA });
my $expected = qq{'abc\123\\\\\\\\def\\\\000ghi'};
is( $result, $expected, 'quote properly handles bytea strings.');

$sth->finish();

cleanup_database($dbh,'test');
$dbh->disconnect();
