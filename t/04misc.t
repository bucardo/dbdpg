#!perl -w

# Various stuff that does not go elsewhere

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
ok( defined $dbh, "Connect to database for miscellaneous tests");

if (DBD::Pg::_pg_use_catalog($dbh)) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
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

