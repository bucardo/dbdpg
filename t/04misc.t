#!perl -w

# Various stuff that does not go elsewhere

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 6;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for miscellaneous tests");

if (DBD::Pg::_pg_use_catalog($dbh)) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}

# Attempt to test whether or not we can get unicode out of the database
SKIP: {
	eval "use Encode;";
	skip "Encode module is needed for unicode tests", 4 if $@;
	my $SQL = "SELECT id, pname FROM dbd_pg_test WHERE id = ?";
	my $sth = $dbh->prepare($SQL);
	$sth->execute(1);
	local $dbh->{pg_enable_utf8} = 1;
	my $utf8_str = chr(0x100).'dam';	# LATIN CAPITAL LETTER A WITH MACRON
	$SQL = "INSERT INTO dbd_pg_test (id, pname, val) VALUES (40, '$utf8_str', 'Orange')";
	is( $dbh->do($SQL), '1', 'Able to insert unicode character into the database');
	$sth->execute(40);
	my ($id, $name) = $sth->fetchrow_array();
	ok( Encode::is_utf8($name), 'Able to read unicode (utf8) data from the database');
	is( length($name), 4, 'Unicode (utf8) data returned from database is not corrupted');
	$sth->finish();
	$sth->execute(1);
	my ($id2, $name2) = $sth->fetchrow_array();
	ok( !Encode::is_utf8($name2), 'ASCII text returned from database does not have utf8 bit set');
	$sth->finish();
}


#
# Test of the "data_sources" method
#

my @result = DBI->data_sources('Pg');
# This may fail due to the wrong port, etc.
if (defined $result[0]) {
	is (grep (/^dbi:Pg:dbname=template1$/, @result), '1', 'The data_sources() method returns a template1 listing');
}
else {
	pass("The data_sources() method returned undef");
}

$dbh->disconnect();

