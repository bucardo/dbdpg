#!perl -w

# Quick test of some bytea handling

use Test::More;
use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);
use strict;
$|=1;

if (defined $ENV{DBI_DSN}){
	plan tests => 7;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for bytea testing");

my $sth;

$sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,bytetest) VALUES (?,?)});
$sth->bind_param(2, undef, { pg_type => DBD::Pg::PG_BYTEA });
ok($sth->execute(400, "a\0b"), 'bytea insert test with string containing null');
ok($sth->execute(401, '\''), 'bytea insert test with string containing a single quote');
ok($sth->execute(402, '\''), 'bytea (second) insert test with string containing a single quote');

$sth = $dbh->prepare(qq{SELECT bytetest FROM dbd_pg_test WHERE id=?});
$sth->execute(400);

my $byte = $sth->fetchall_arrayref()->[0][0];
ok($byte eq "a\0b", "text from BYTEA column looks corect");

$sth->execute(402);
$byte = $sth->fetchall_arrayref()->[0][0];
is($byte, '\'', 'text from BYTEA column with quote');


$sth->finish();

$dbh->rollback();
ok( $dbh->disconnect(), 'Disconnect from database');
