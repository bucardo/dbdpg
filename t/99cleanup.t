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

# Remove the test table if it exists
my $schema = DBD::Pg::_pg_use_catalog($dbh);
my $SQL = "SELECT COUNT(*) FROM pg_class WHERE relname='dbd_pg_test'";
if ($schema) {
	$schema = exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public';
	$dbh->do("SET search_path TO " . $dbh->quote_identifier($schema));
	$SQL = "SELECT COUNT(*) FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n ".
		"WHERE c.relnamespace=n.oid AND c.relname='dbd_pg_test' AND n.nspname=".
			$dbh->quote($schema);
}

# Implicit tests of prepare, execute, fetchall_arrayref, and do
my $sth = $dbh->prepare($SQL);
$sth->execute();
my $count = $sth->fetchall_arrayref()->[0][0];
if ($count == 1) {
	$dbh->do("DROP TABLE dbd_pg_test");
}

pass('The testing table "dbd_pg_test" has been dropped');


$dbh->disconnect();

