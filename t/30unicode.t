#!perl

## Test everything related to Unicode.
## At the moment, this basically means testing the UTF8 client_encoding
## and $dbh->{pg_enable_utf8} bits

use 5.006;
use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 1;

isnt ($dbh, undef, 'Connect to database for unicode testing');

my $pgversion = $dbh->{pg_server_version};

my $t;

cleanup_database($dbh,'test');
$dbh->disconnect();
