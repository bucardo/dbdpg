#!perl

## Test everything related to Unicode.
## At the moment, this basically means testing the UTF8 client_encoding
## and $dbh->{pg_enable_utf8} bits

use 5.006;
use strict;
use warnings;
use utf8;
use Encode;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

isnt ($dbh, undef, 'Connect to database for unicode testing');

my $pgversion = $dbh->{pg_server_version};

my $t;

my $name = 'Émilie du Châtelet';
utf8::encode($name);

my $SQL = 'SELECT ?::text';
my $sth = $dbh->prepare($SQL);
$sth->execute($name);
my $result = $sth->fetchall_arrayref->[0][0];
$t = 'Fetching UTF-8 string from the database returns proper string';
is ($result, $name, $t);
$t = 'Fetching UTF-8 string from the database returns string with UTF-8 flag on';
ok (utf8::is_utf8($result), $t);

$dbh->{pg_enable_utf8} = 0;
$sth->execute($name);
$result = $sth->fetchall_arrayref->[0][0];
$t = 'Fetching UTF-8 string from the database returns proper string (pg_enable_utf8=0)';
is ($result, $name, $t);
$t = 'Fetching UTF-8 string from the database returns string with UTF-8 flag off (pg_enable_utf8=0)';
ok (!utf8::is_utf8($result), $t);


$name = 'Ada Lovelace';
utf8::encode($name);

$dbh->{pg_enable_utf8} = -1;
$SQL = 'SELECT ?::text';
$sth = $dbh->prepare($SQL);
$sth->execute($name);
$result = $sth->fetchall_arrayref->[0][0];
$t = 'Fetching ASCII string from the database returns proper string';
is ($result, $name, $t);
$t = 'Fetching ASCII string from the database returns string with UTF-8 flag on';
ok (utf8::is_utf8($result), $t);

$dbh->{pg_enable_utf8} = 0;
$sth->execute($name);
$result = $sth->fetchall_arrayref->[0][0];
$t = 'Fetching ASCII string from the database returns proper string (pg_enable_utf8=0)';
is ($result, $name, $t);
$t = 'Fetching ASCII string from the database returns string with UTF-8 flag off (pg_enable_utf8=0)';
ok (!utf8::is_utf8($result), $t);


cleanup_database($dbh,'test');
$dbh->disconnect();

done_testing();

