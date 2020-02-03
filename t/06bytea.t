#!perl

## Test bytea handling

use 5.008001;
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
plan tests => 36;

isnt ($dbh, undef, 'Connect to database for bytea testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
    $dbh->do('SET escape_string_warning = false');
}

my ($sth, $t);

$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});

$t='bytea insert test with string containing null and backslashes';
$sth->bind_param(1, undef, { pg_type => PG_INT4 });
$sth->bind_param(2, undef, { pg_type => PG_BYTEA });
ok ($sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), $t);

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

if ($pgversion < 90000) {
    test_outputs(undef);
    SKIP: { skip 'No BYTEA output format setting before 9.0', 13 }
}
else {
    test_outputs($_) for qw(hex escape);
}

$sth->finish();

cleanup_database($dbh,'test');
$dbh->disconnect();

sub test_outputs {
    my $output = shift;
    $dbh->do(qq{SET bytea_output = '$output'}) if $output;

    $t='Received correct text from BYTEA column with backslashes';
    $t.=" ($output output)" if $output;
    $sth = $dbh->prepare(q{SELECT bytetest FROM dbd_pg_test WHERE id=?});
    $sth->execute(400);
    my $byte = $sth->fetchall_arrayref()->[0][0];
    is ($byte, 'aa\bb\cc\\\0dd\\', $t);

    $t='Received correct text from BYTEA column with quote';
    $t.=" ($output output)" if $output;
    $sth->execute(402);
    $byte = $sth->fetchall_arrayref()->[0][0];
    is ($byte, '\'', $t);

    $t='Ensure proper handling of high bit characters';
    $t.=" ($output output)" if $output;
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

    $t='quote properly handles bytea strings';
    $t.=" ($output output)" if $output;
    my $string = "abc\123\\def\0ghi";
    my $result = $dbh->quote($string, { pg_type => PG_BYTEA });
    my $E = $pgversion >= 80100 ? q{E} : q{};
    my $expected = qq{${E}'abc\123\\\\\\\\def\\\\000ghi'};
    is ($result, $expected, $t);
    is ($dbh->quote($string, SQL_BLOB), $expected);
    is ($dbh->quote($string, SQL_BINARY), $expected);
    is ($dbh->quote($string, SQL_VARBINARY), $expected);
    is ($dbh->quote($string, SQL_LONGVARBINARY), $expected);
    return;
}
