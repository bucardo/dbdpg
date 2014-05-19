#!perl

## Test everything related to Unicode.
## At the moment, this basically means testing the UTF8 client_encoding
## and $dbh->{pg_enable_utf8} bits

use 5.006;
use strict;
use warnings;
use utf8;
use charnames ':full';
use Encode qw(encode_utf8);
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

isnt ($dbh, undef, 'Connect to database for unicode testing');

my $t;

my $name_d = my $name_u = "\N{LATIN CAPITAL LETTER E WITH ACUTE}milie du Ch\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}telet";
utf8::downgrade($name_d);
utf8::upgrade($name_u);

# Before 5.12.0 the text to the left of => gets to be SvUTF8() under use utf8;
# even if it's plain ASCII. This would confuse what we test for below.
foreach (
    [upgraded => 'text' => $name_u],
    [downgraded => 'text' => $name_d],
    [upgraded => 'text[]' => [$name_u]],
    [downgraded => 'text[]' => [$name_d]],
    [mixed => 'text[]' => [$name_d,$name_u]],
) {
    my ($state, $type, $value) = @$_;
    foreach (
        [placeholder => "SELECT ?::$type", $value],
        (($type eq 'text') ? (
            [interpolated => "SELECT '$value'::$type"],
        ):()),
    ) {
        foreach my $enable_utf8 (1, 0, -1) {
            my ($qtype, $sql, @args) = @$_;
            my $desc = "$state UTF-8 $qtype $type (pg_enable_utf8=$enable_utf8)";
            is(utf8::is_utf8($sql), ($state eq 'upgraded'), "$desc query has correct flag")
                if $qtype eq 'interpolated';
            if ($state ne 'mixed') {
                foreach my $arg (map { ref($_) ? @{$_} : $_ } @args) {
                    is(utf8::is_utf8($arg), ($state eq 'upgraded'), "$desc arg has correct flag")
                }
            }
            $dbh->{pg_enable_utf8} = $enable_utf8;

            my $sth = $dbh->prepare($sql);
            $sth->execute(@args);
            my $result = $sth->fetchall_arrayref->[0][0];
            my $return_value =
                $enable_utf8 ? $value :
                ref $value   ? [ map encode_utf8($_), @{$value} ] :
                encode_utf8($value);
            $t = "$desc returns proper value";
            is_deeply ($result, $return_value, $t);
            $t = "$desc returns string with correct UTF-8 flag ";
            is(utf8::is_utf8($_), !!$enable_utf8, $t) for (ref $result ? @{$result} : $result);
        }
    }
}

$t = 'Generated string is not utf8';
my $name = 'Ada Lovelace';
utf8::encode($name);
ok (!utf8::is_utf8($name), $t);

$dbh->{pg_enable_utf8} = -1;
my $SQL = 'SELECT ?::text';
my $sth = $dbh->prepare($SQL);
$sth->execute($name);
my $result = $sth->fetchall_arrayref->[0][0];
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

$dbh->{pg_enable_utf8} = 1;
my $before = "\N{WHITE SMILING FACE}";
my ($after) = $dbh->selectrow_array('SELECT ?::text', {}, $before);
is($after, $before, 'string is the same after round trip');
ok(utf8::is_utf8($after), 'string has utf8 flag set');

cleanup_database($dbh,'test');
$dbh->disconnect();

done_testing();

