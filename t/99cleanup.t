#!perl -w

# Cleanup by removing the test table

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 2;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, 'Connect to database for cleanup');

ok( $dbh->do('DROP TABLE dbd_pg_test'), 'The testing table "dbd_pg_test" has been dropped');

$dbh->disconnect();

