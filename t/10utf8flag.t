#!perl

## Test the pg_utf8_flag handling

use 5.006;
use strict;
use warnings;
use Test::More;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 16;

isnt ($dbh, undef, 'Connect to database for pg_utf8_flag testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
	$dbh->do('SET escape_string_warning = false');
}

my ($sth, $t);

$t='pg_utf8_flag starts life with sane default value';

$t='pg_utf8_flag can be set on startup to true';

$t='pg_utf8_flag can be set on startup to false';

$t='pg_utf8_flag can be set on startup to -1';

$t='pg_utf8_flag can be set after startup to true';

$t='pg_utf8_flag can be set after startup to false';

$t='pg_utf8_flag can be set after startup to -1';


$t='A UTF-8 database sets utf8_flag correctly';

$t='A UTF-8 database returns flagged data when client_encoding is UTF-8';

$t='A UTF-8 database returns raw data when client_encoding is UTF-8 and pg_utf8_flag off';

$t='A UTF-8 database returns raw data when client_encoding is not UTF-8';

$t='A UTF-8 database returns flagged data when client_encoding is not UTF-8 and pg_utf8_flag on';


$t='A SQL_ASCII database sets utf8_flag correctly';

$t='A SQL_ASCII database returns raw data when client_encoding is UTF-8';

$t='A SQL_ASCII database returns raw data when client_encoding is UTF-8 and pg_utf8_flag off';

$t='A SQL_ASCII database returns raw data when client_encoding is not UTF-8';

$t='A SQL_ASCII database returns raw data when client_encoding is not UTF-8 and pg_utf8_flag on';


$t='A BIG5 database sets utf8_flag correctly';

$t='A BIG5 database returns flagged data when client_encoding is UTF-8';

$t='A BIG5 database returns raw data when client_encoding is UTF-8 and pg_utf8_flag off';

$t='A BIG5 database returns raw data when client_encoding is not UTF-8';

$t='A BIG5 database returns flagged data when client_encoding is not UTF-8 and pg_utf8_flag on';


exit;
