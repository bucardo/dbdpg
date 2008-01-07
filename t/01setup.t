#!perl -w

# Create the "dbd_pg_test" table and the "dbd_pg_sequence" sequence.
# Because the table is used for the other tests, we bail out if we cannot create it.

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 4;
}
else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file.';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 0, PrintError => 0, AutoCommit => 1});
ok( defined $dbh, "Connect to database for test table creation");

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

# Implicit tests of prepare, execute, fetchall_arrayref, and do
my $sth = $dbh->prepare($SQL);
$sth->execute('dbd_pg_test');
my $count = $sth->fetchall_arrayref()->[0][0];
if (1==$count) {
	$dbh->do(sprintf "DROP TABLE %s%s", $schema ? "$schema." : '', 'dbd_pg_test');
}
$sth->execute('dbd_pg_sequence');
$count = $sth->fetchall_arrayref()->[0][0];
if (1==$count) {
	$dbh->do(sprintf "DROP SEQUENCE %s%s", $schema ? "$schema." : '', 'dbd_pg_sequence');
}
$SQL = "SELECT COUNT(*) FROM pg_class WHERE relname=?";
$sth = $dbh->prepare($SQL);
$sth->execute('dbd_pg_litest');
$count = $sth->fetchall_arrayref()->[0][0];
if (1==$count) {
	$dbh->do("DROP TABLE dbd_pg_testli.dbd_pg_litest");
}
$sth->execute('dbd_pg_testseq');
$count = $sth->fetchall_arrayref()->[0][0];
if (1==$count) {
	$dbh->do("DROP SEQUENCE dbd_pg_testli.dbd_pg_testseq");
}

# Remove test schemas
$SQL = "SELECT COUNT(*) FROM pg_namespace WHERE nspname=?";
$sth = $dbh->prepare($SQL);
$sth->execute('dbd_pg_testli');
$count = $sth->fetchall_arrayref()->[0][0];
if (1==$count) {
	$dbh->do("DROP SCHEMA dbd_pg_testli CASCADE");
}

$dbh->do("CREATE SEQUENCE dbd_pg_sequence");
# If you add columns to this, please do not use reserved words!
$SQL = qq{
CREATE TABLE dbd_pg_test (
  id         integer not null primary key,
  lii        integer unique not null default nextval('dbd_pg_sequence'),
  pname      varchar(20) default 'Testing Default' ,
  val        text,
  score      float CHECK(score IN ('1','2','3')),
  Fixed      character(5),
  pdate      timestamp default now(),
  testarray  text[][],
  testarray2 int[],
  "CaseTest" boolean,
  bytetest   bytea
)
};

$dbh->{Warn}=0;
ok( $dbh->do($SQL), qq{Created test table "dbd_pg_test"})
	or print STDOUT "Bail out! Test table could not be created: $DBI::errstr\n";

$dbh->do("COMMENT ON COLUMN dbd_pg_test.id IS 'Bob is your uncle'");

# Double check that the file is there
$sth = $dbh->prepare("SELECT 1 FROM pg_class WHERE relname = 'dbd_pg_test'");
$sth->execute();
$count = $sth->fetchall_arrayref()->[0][0];
is( $count, 1, 'Test table was successfully created')
	or print STDOUT "Bail out! Test table was not created\n";

ok( $dbh->disconnect(), 'Disconnect from database');
