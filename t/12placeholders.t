#!perl -w

# Test of placeholders

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 9;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, 'Connect to database for placeholder testing');
$dbh->trace($ENV{DBD_TRACE}) if exists $ENV{DBD_TRACE};

if (DBD::Pg::_pg_use_catalog($dbh)) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}

my $quo = $dbh->quote("\\'?:");
my $sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,pname) VALUES (100,$quo)});
$sth->execute();

my $sql = "SELECT pname FROM dbd_pg_test WHERE pname = $quo";
$sth = $dbh->prepare($sql);
$sth->execute();

my ($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'fetch');

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with one bind param where none expected');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = ?";
$sth = $dbh->prepare($sql);
$sth->execute("\\'?:");

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'execute with ? placeholder');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = :1";
$sth = $dbh->prepare($sql);
$sth->bind_param(":1", "\\'?:");
$sth->execute();

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'execute with :1 placeholder');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = '?'";

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = ':1'";

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted :1');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'";
$sth = $dbh->prepare($sql);

eval {
## XX ???
	local $dbh->{PrintError} = 0;
	local $sth->{PrintError} = 0;
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

$sth->finish();
$dbh->rollback();

ok( $dbh->disconnect(), 'Disconnect from database');

