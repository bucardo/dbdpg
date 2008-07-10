#!perl

## Test bytea handling

use 5.006;
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

isnt ($dbh, undef, 'Connect to database for bytea testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

my ($sth, $t);

$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest) VALUES (?,?)});

$t='bytea insert test with string containing null and backslashes';
$sth->bind_param(2, undef, { pg_type => PG_BYTEA });
ok ($sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), $t);

$t='bytea insert test with string containing a single quote';
ok ($sth->execute(401, '\''), $t);

$t='bytea (second) insert test with string containing a single quote';
ok ($sth->execute(402, '\''), $t);

$t='Received correct text from BYTEA column with backslashes';
$sth = $dbh->prepare(q{SELECT bytetest FROM dbd_pg_test WHERE id=?});
$sth->execute(400);
my $byte = $sth->fetchall_arrayref()->[0][0];
is ($byte, 'aa\bb\cc\\\0dd\\', $t);

$t='Received correct text from BYTEA column with quote';
$sth->execute(402);
$byte = $sth->fetchall_arrayref()->[0][0];
is ($byte, '\'', $t);

$t='quote properly handles bytea strings';
my $string = "abc\123\\def\0ghi";
my $result = $dbh->quote($string, { pg_type => PG_BYTEA });
my $E = $pgversion >= 80100 ? q{E} : q{};
my $expected = qq{${E}'abc\123\\\\\\\\def\\\\000ghi'};
is ($result, $expected, $t);

$sth->finish();

cleanup_database($dbh,'test');
$dbh->disconnect();
