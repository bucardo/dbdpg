#!perl -w

# Cleanup by removing the test table

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 3;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 1});
ok( defined $dbh, 'Connect to database for cleanup');

# Remove the test relations if they exist
my $schema = DBD::Pg::_pg_use_catalog($dbh);
my $SQL = "SELECT COUNT(*) FROM pg_class WHERE relname=?";
if ($schema) {
	$schema = exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public';
	$dbh->do("SET search_path TO " . $dbh->quote_identifier($schema));
	$SQL = "SELECT COUNT(*) FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n ".
		"WHERE c.relnamespace=n.oid AND c.relname=? AND n.nspname=".
			$dbh->quote($schema);
}
my $sth = $dbh->prepare($SQL);

for (3,2,1,'') {
	my $table = "dbd_pg_test$_";
	$sth->execute($table);
	if (1==$sth->fetchall_arrayref()->[0][0]) {
		$dbh->do(sprintf "DROP TABLE %s$table", $schema ? "$schema." : '');
	}
}

$sth->execute('dbd_pg_sequence');
if (1==$sth->fetchall_arrayref()->[0][0]) {
	$dbh->do(sprintf "DROP SEQUENCE %s%s", $schema ? "$schema." : '', 'dbd_pg_sequence');
}

pass('All testing tables (e.g. "dbd_pg_test*") have been dropped');
pass('The testing sequence "dbd_pg_sequence" has been dropped');


$dbh->disconnect();

