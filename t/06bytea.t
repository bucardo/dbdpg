#!perl -w

# Quick test of some bytea handling

use Test::More;
use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 3;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for bytea testing");

my $sth;

$sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,bytetest) VALUES (?,?)});
$sth->bind_param(1, undef, SQL_INTEGER);
$sth->bind_param(2, undef, { pg_type => DBD::Pg::PG_BYTEA });
$sth->execute(400, "a\0b");
$sth = $dbh->prepare(qq{SELECT bytetest FROM dbd_pg_test WHERE id=?});
$sth->execute(400);

my $byte = $sth->fetchall_arrayref()->[0][0];
ok($byte eq "a\0b", "text from BYTEA column looks corect");
$sth->finish();

$dbh->rollback();
ok( $dbh->disconnect(), 'Disconnect from database');
