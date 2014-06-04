#!perl

## Test bytea handling

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
plan tests => 46;

isnt ($dbh, undef, 'Connect to database for bytea testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
	$dbh->do('SET escape_string_warning = false');
}

foreach my $type_str ('SQL_VARBINARY', '{ TYPE => SQL_VARBINARY }', '{ pg_type => PG_BYTEA }') {
    my $type = eval $type_str or die $@;
    test_inserts($type, $type_str);
    if ($pgversion < 90000) {
        test_outputs($type, $type_str, undef);
        SKIP: { skip 'No BYTEA output format setting before 9.0', 5 }
    }
    else {
        test_outputs($type, $type_str, $_) for qw(hex escape);
    }
    $dbh->do('delete from dbd_pg_test');
}

cleanup_database($dbh,'test');
$dbh->disconnect();

my ($t, $binary_out);

sub test_inserts {
    my $type = shift;
    my $type_str = shift;

    my $sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id,bytetest,bytearray,testarray2) VALUES (?,?,'{1,2,3}','{5,6,7}')});

    $t="bytea insert test with string containing null and backslashes (type: $type_str)";
    $sth->bind_param(1, undef, { pg_type => PG_INT4 });
    $sth->bind_param(2, undef, $type);
    ok ($sth->execute(400, 'aa\\bb\\cc\\\0dd\\'), $t);

    $t='bytea insert test with string containing a single quote';
    ok ($sth->execute(401, '\''), $t);

    $t='bytea (second) insert test with string containing a single quote';
    ok ($sth->execute(402, '\''), $t);

    $binary_out = '';
    $t='store binary data in BYTEA column';
    for (my $i=0; $i<256; $i++) {
        $binary_out .= chr($i);
    }
    $sth->{pg_server_prepare} = 0;
    ok ($sth->execute(403, $binary_out), $t);
    $sth->{pg_server_prepare} = 1;
    ok ($sth->execute(404, $binary_out), $t);
}

sub test_outputs {
    my $type = shift;
    my $type_str = shift;
    my $output = shift;
    $dbh->do(qq{SET bytea_output = '$output'}) if $output;

    $t='Received correct text from BYTEA column with backslashes';
    $t.=" ($output output)" if $output;
    my $sth = $dbh->prepare(q{SELECT bytetest FROM dbd_pg_test WHERE id=?});
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
    my ($binary_in) = $sth->fetchrow_array();
    cmp_ok ($binary_in, 'eq', $binary_out, $t);
    $sth->execute(404);
    ($binary_in) = $sth->fetchrow_array();
    ok ($binary_in eq $binary_out, $t);

    $t='quote properly handles bytea strings';
    $t.=" ($output output)" if $output;
    my $string = "abc\123\\def\0ghi";
    my $result = $dbh->quote($string, $type);
    my $E = $pgversion >= 80100 ? q{E} : q{};
    my $expected = qq{${E}'abc\123\\\\\\\\def\\\\000ghi'};
    is ($result, $expected, "$t (type: $type_str)");
    return;
}
