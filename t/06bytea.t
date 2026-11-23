#!perl

## Test bytea handling

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 49;

isnt ($dbh, undef, 'Connect to database for bytea testing');

my $pgversion = $dbh->{pg_server_version};
if ($pgversion >= 80100 and $pgversion < 19000) {
    $dbh->do('SET escape_string_warning = false');
}

my ($sth, $t);

$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});

$t='bytea insert test with string containing null and backslashes';
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, { pg_type => PG_BYTEA });
ok ($sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), $t); ## nospellcheck

$t='bytea insert test with string containing a single quote';
ok ($sth->execute(401, '\''), $t);

$t='bytea (second) insert test with string containing a single quote';
ok ($sth->execute(402, '\''), $t);

my ($binary_in, $binary_out);
$t='store binary data in BYTEA column';
for(my $i=0; $i<256; $i++) { $binary_out .= chr($i); }
$sth->{pg_server_prepare} = 0;
ok ($sth->execute(403, $binary_out), $t);
$sth->{pg_server_prepare} = 1;
ok ($sth->execute(404, $binary_out), $t);

$t='store binary data in BYTEA column via SQL_BLOB';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, SQL_BLOB);
ok ($sth->execute(405, $binary_out), $t);

$t='store binary data in BYTEA column via SQL_BINARY';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, SQL_BINARY);
ok ($sth->execute(406, $binary_out), $t);

$t='store binary data in BYTEA column via SQL_VARBINARY';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, SQL_VARBINARY);
ok ($sth->execute(407, $binary_out), $t);

$t='store binary data in BYTEA column via SQL_LONGVARBINARY';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, SQL_LONGVARBINARY);
ok ($sth->execute(408, $binary_out), $t);

# array tests

$t='store binary array data in bytea[] column';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytearray) VALUES (?,?)});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
# without bind_param, special characters need to be escaped; 012 is newline
ok ($sth->execute(500, [1, 'foo\012bar']), $t);

$t='store mixed printable/non-printable characters in bytea[] column';
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytearray) VALUES (?,?)});
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
ok ($sth->execute(501, [1,'foo',
            'null\000tab\011escape\033backslash\\\\double quote"single quote\'high bytes\200\234\277\305\340\377'
        ]), $t);


# Set DBDPG_TEST_BYTEA_HEX environment variable to 1 or 0 to force
# enable/disable testing of hex quoting.
my $supports_hex = $ENV{DBDPG_TEST_BYTEA_HEX};
if (! defined($supports_hex)) {
    $supports_hex = $pgversion >= 90000;
}

if ($supports_hex) {
    $t='store hex-escaped characters in a bytea[] column';
    $sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytearray) VALUES (?,?)});
    $sth->bind_param(1, undef, { pg_type => PG_INT4 });
    ok ($sth->execute(550, [1,'foo', '\x00104A8FABDEFF' ]), $t);

    test_outputs($supports_hex, $_) for qw(hex escape);
}
else {
    test_outputs($supports_hex, undef);
    SKIP: { skip 'No BYTEA hex format support before 9.0', 21 }
}

$sth->finish();

cleanup_database($dbh,'test');
$dbh->disconnect();

sub test_outputs {
    my $test_hex = shift;
    my $output = shift;
    my $t_append = '';
    if ($output) {
        $dbh->do(qq{SET bytea_output = '$output'});
        $t_append = " ($output output)";
    }

    $t="Received correct text from BYTEA column with backslashes$t_append";
    $sth = $dbh->prepare(q{SELECT bytetest FROM dbd_pg_test WHERE id=?});
    $sth->execute(400);
    my $byte = $sth->fetchall_arrayref()->[0][0];
    is ($byte, 'aa\bb\cc\\\0dd\\', $t); ## nospellcheck

    $t="Received correct text from BYTEA column with quote$t_append";
    $sth->execute(402);
    $byte = $sth->fetchall_arrayref()->[0][0];
    is ($byte, '\'', $t);

    $t="Ensure proper handling of high bit characters$t_append";
    $sth->execute(403);
    ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);
    $sth->execute(404);
    ($binary_in) = $sth->fetchrow_array();
    ok ($binary_in eq $binary_out, $t);
    $sth->execute(405);
    ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);
    $sth->execute(406);
    ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);
    $sth->execute(407);
    ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);
    $sth->execute(408);
    ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);

    $t="Received correct representation of bytea[] data$t_append";
    $sth = $dbh->prepare(q{SELECT bytearray FROM dbd_pg_test WHERE id=?});
    $sth->execute(500);
    ($binary_in) = $sth->fetchrow_array();
    is_deeply ($binary_in, [1, "foo\nbar"], $t);

    $t="Received correct representation of mixed printable/non-printable bytea[] data$t_append";
    $sth = $dbh->prepare(q{SELECT bytearray FROM dbd_pg_test WHERE id=?});
    $sth->execute(501);
    ($binary_in) = $sth->fetchrow_array();
    is_deeply ($binary_in, [1, 'foo', "null\0tab\tescape\033backslash\\double quote\"single quote'high bytes\200\234\277\305\340\377"], $t);

    if ($test_hex) {
        $t="Received correct representation of bytea[] data inserted via hex-escape$t_append";
        $sth = $dbh->prepare(q{SELECT bytearray FROM dbd_pg_test WHERE id=?});
        $sth->execute(550);
        ($binary_in) = $sth->fetchrow_array();
        is_deeply ($binary_in, [1, 'foo', "\x00\x10\x4A\x8F\xAB\xDE\xFF"]);
    }

    $t="quote properly handles bytea strings$t_append";
    my $string = "abc\123\\def\0ghi";
    my $result = $dbh->quote($string, { pg_type => PG_BYTEA });
    my $E = $pgversion >= 80100 ? q{E} : q{};
    my $expected = qq{${E}'abc\123\\\\\\\\def\\\\000ghi'};
    is ($result, $expected, $t);
    is ($dbh->quote($string, SQL_BLOB), $expected, "$t (SQL_BLOB)");
    is ($dbh->quote($string, SQL_BINARY), $expected, "$t (SQL_BINARY)");
    is ($dbh->quote($string, SQL_VARBINARY), $expected, "$t (SQL_VARBINARY)");
    is ($dbh->quote($string, SQL_LONGVARBINARY), $expected, "$t (SQL_LONGVARBINARY)");

    $t="quote properly handles bytea escaped binary in arrays$t_append";
    my $array = ['abc', '\000', 10];
    $result = $dbh->quote($array);
    $expected = qq{${E}'{"abc","\\\\\\\\000","10"}'};
    is ($result, $expected, $t);

    if ($test_hex) {
        $t="quote properly handles bytea hex-escaped binary in arrays$t_append";
        $array = ['abc', '\x00507080ABFF', 10];
        $result = $dbh->quote($array);
        $expected = qq{${E}'{"abc","\\\\\\\\x00507080ABFF","10"}'};
        is ($result, $expected, $t);
    }

    return;
}
