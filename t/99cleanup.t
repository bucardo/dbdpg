#!perl

## Cleanup all database objects we may have created

use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

if (defined $ENV{DBI_DSN}) {
	plan tests => 1;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = connect_database({nosetup => 1});
ok( defined $dbh, 'Connect to database for cleanup');

cleanup_database($dbh);
$dbh->disconnect();

