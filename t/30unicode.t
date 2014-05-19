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


my @tests;

# Beware, characters used for testing need to be known to Unicode version 4.0.0,
# which is what perl 5.8.1 shipped with.
foreach (
    [ascii => 'Ada Lovelace'],
    ['latin 1 range' => "\N{LATIN CAPITAL LETTER E WITH ACUTE}milie du Ch\N{LATIN SMALL LETTER A WITH CIRCUMFLEX}telet"],
    # I'm finding it awkward to continue the theme of female mathematicians
    ['base plane' => "Interrobang\N{INTERROBANG}"],
    ['astral plane' => "\N{MUSICAL SYMBOL CRESCENDO}"],
     ) {
    my ($range, $text) = @$_;
    my $name_d = my $name_u = $text;
    utf8::upgrade($name_u);
    # Before 5.12.0 the text to the left of => gets to be SvUTF8() under use utf8;
    # even if it's plain ASCII. This would confuse what we test for below.
    push @tests, (
        [upgraded => $range => 'text' => $name_u],
        [upgraded => $range => 'text[]' => [$name_u]],
    );
    if (utf8::downgrade($name_d, 1)) {
        push @tests, (
            [downgraded => $range => 'text' => $name_d],
            [downgraded => $range => 'text[]' => [$name_d]],
            [mixed => $range => 'text[]' => [$name_d,$name_u]],
        );
    }
}

foreach (@tests) {
    my ($state, $range, $type, $value) = @$_;
    foreach my $test (
        {
            qtype => 'placeholder',
            sql => "SELECT ?::$type",
            args => [$value],
        },
        (($type eq 'text') ? (
            {
                qtype => 'interpolated',
                sql => "SELECT '$value'::$type",
            },
            # Test that what we send is the same as the database's idea of characters:
            {
                qtype => 'placeholder length',
                sql => "SELECT length(?::$type)",
                args => [$value],
                want => length($value),
            },
            {
                qtype => 'interpolated length',
                sql => "SELECT length('$value'::$type)",
                want => length($value),
            },
        ):()),
    ) {
        foreach my $enable_utf8 (1, 0, -1) {
            my $desc = "$state $range UTF-8 $test->{qtype} $type (pg_enable_utf8=$enable_utf8)";
            my @args = @{$test->{args} || []};
            my $want = exists $test->{want} ? $test->{want} : $value;
            if (!$enable_utf8) {
                $want = ref $want ? [ map encode_utf8($_), @{$want} ]
                    : encode_utf8($want);
            }

            is(utf8::is_utf8($test->{sql}), ($state eq 'upgraded'), "$desc query has correct flag")
                if $test->{qtype} =~ /^interpolated/;
            if ($state ne 'mixed') {
                foreach my $arg (map { ref($_) ? @{$_} : $_ } @args) {
                    is(utf8::is_utf8($arg), ($state eq 'upgraded'), "$desc arg has correct flag")
                }
            }
            $dbh->{pg_enable_utf8} = $enable_utf8;

            my $sth = $dbh->prepare($test->{sql});
            $sth->execute(@args);
            my $result = $sth->fetchall_arrayref->[0][0];
            is_deeply ($result, $want,
                       "$desc returns proper value");
            unless ($test->{qtype} =~ /length$/) {
                # Whilst XS code can set SVf_UTF8 on an IV, the core's SV
                # copying code doesn't copy it. So we can't assume that numeric
                # values we see "out here" still have it set. Hence skip this
                # test for the SQL length() tests.
                is(utf8::is_utf8($_), !!$enable_utf8, "$desc returns string with correct UTF-8 flag")
                    for (ref $result ? @{$result} : $result);
            }
        }
    }
}

my $t = 'Generated string is not utf8';
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

