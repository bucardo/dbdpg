#!perl -w

## Test bytea handling

use Test::More;
use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);
use strict;
$|=1;

if (defined $ENV{DBI_DSN}){
	plan tests => 8;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for bytea testing");

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do("SET escape_string_warning = false");
}

my $sth;

$sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,bytetest) VALUES (?,?)});

$sth->bind_param(2, undef, { pg_type => PG_BYTEA });
ok($sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), 'bytea insert test with string containing null and backslashes');
ok($sth->execute(401, '\''), 'bytea insert test with string containing a single quote');
ok($sth->execute(402, '\''), 'bytea (second) insert test with string containing a single quote');

$sth = $dbh->prepare(qq{SELECT bytetest FROM dbd_pg_test WHERE id=?});

$sth->execute(400);
my $byte = $sth->fetchall_arrayref()->[0][0];
is($byte, 'aa\bb\cc\\\0dd\\', 'Received correct text from BYTEA column with backslashes');

$sth->execute(402);
$byte = $sth->fetchall_arrayref()->[0][0];
is($byte, '\'', 'Received correct text from BYTEA column with quote');

my $string = "abc\123\\def\0ghi";
my $result = $dbh->quote($string, { pg_type => PG_BYTEA });
my $expected = qq{'abc\123\\\\\\\\def\\\\000ghi'};
is ($result, $expected, 'quote properly handles bytea strings.');

$sth->finish();

$dbh->rollback();
ok( $dbh->disconnect(), 'Disconnect from database');
