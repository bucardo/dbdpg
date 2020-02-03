#!perl

## Test all handle attributes: database, statement, and generic ("any")

use 5.008001;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use DBI     ':sql_types';
use DBD::Pg qw/ :pg_types :async /;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my ($helpconnect,$connerror,$dbh) = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 273;

isnt ($dbh, undef, 'Connect to database for handle attributes testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});

my $attributes_tested = q{

d = database handle specific
s = statement handle specific
b = both database and statement handle
a = any type of handle (but we usually use database)

In order:

d Statement (must be the first one tested)
d CrazyDiamond (bogus)
d private_dbdpg_*
d AutoCommit
d Driver
d Name
d RowCacheSize
d Username
d PrintWarn
d pg_INV_READ
d pg_INV_WRITE
d pg_protocol
d pg_errorlevel
d pg_bool_tf
d pg_db
d pg_user
d pg_pass
d pg_port
d pg_default_port
d pg_options
d pg_socket
d pg_pid
d pg_standard_conforming strings
d pg_enable_utf8
d Warn

d pg_prepare_now - tested in 03smethod.t
d pg_server_prepare - tested in 03smethod.t
d pg_switch_prepared - tested in 03smethod.t
d pg_prepare_now - tested in 03smethod.t
d pg_placeholder_dollaronly - tested in 12placeholders.t

s NUM_OF_FIELDS, NUM_OF_PARAMS
s NAME, NAME_lc, NAME_uc, NAME_hash, NAME_lc_hash, NAME_uc_hash
s TYPE, PRECISION, SCALE, NULLABLE
s CursorName
s Database
s ParamValues
s ParamTypes
s RowsInCache
s pg_size
s pg_type
s pg_oid_status
s pg_cmd_status
b pg_async_status

a Active
a Executed
a Kids
a ActiveKids
a CachedKids
a Type
a ChildHandles
a CompatMode
a PrintError
a RaiseError
a HandleError
a HandleSetErr
a ErrCount
a ShowErrorStatement
a TraceLevel
a FetchHashKeyName
a ChopBlanks
a LongReadLen
a LongTruncOk
a TaintIn
a TaintOut
a Taint
a Profile (not tested)
a ReadOnly

d AutoInactiveDestroy (must be the last one tested)
d InactiveDestroy (must be the last one tested)

};

my ($attrib,$SQL,$sth,$warning,$result,$expected,$t);

# Get the DSN and user from the test file, if it exists
my ($testdsn, $testuser) = get_test_settings();


#
# Test of the database handle attribute "Statement"
#

$SQL = 'SELECT 123';
$sth = $dbh->prepare($SQL);
$sth->finish();

$t='DB handle attribute "Statement" returns the last prepared query';
$attrib = $dbh->{Statement};
is ($attrib, $SQL, $t);

#
# Test of bogus database/statement handle attributes
#

## DBI switched from error to warning in 1.43
$t='Error or warning when setting an invalid database handle attribute';
$warning=q{};
eval {
    local $SIG{__WARN__} = sub { $warning = shift; };
    $dbh->{CrazyDiamond}=1;
};
isnt ($warning, q{}, $t);

$t='Setting a private attribute on a database handle does not throw an error';
eval {
    $dbh->{private_dbdpg_CrazyDiamond}=1;
};
is ($@, q{}, $t);

$sth = $dbh->prepare('SELECT 123');

$t='Error or warning when setting an invalid statement handle attribute';
$warning=q{};
eval {
    local $SIG{__WARN__} = sub { $warning = shift; };
    $sth->{CrazyDiamond}=1;
};
isnt ($warning, q{}, $t);

$t='Setting a private attribute on a statement handle does not throw an error';
eval {
    $sth->{private_dbdpg_CrazyDiamond}=1;
};
is ($@, q{}, $t);

#
# Test of the database handle attribute "AutoCommit"
#

$t='Commit after deleting all rows from dbd_pg_test';
$dbh->do('DELETE FROM dbd_pg_test');
ok ($dbh->commit(), $t);

$t='Connect to database with second database handle, AutoCommit on';
my $dbh2 = connect_database({AutoCommit => 1});
isnt ($dbh2, undef, $t);

$t='Insert a row into the database with first database handle';
ok ($dbh->do(q{INSERT INTO dbd_pg_test (id, pname, val) VALUES (1, 'Coconut', 'Mango')}), $t);

$t='Second database handle cannot see insert from first';
my $rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
is ($rows, 0, $t);

$t='Insert a row into the database with second database handle';
ok ($dbh->do(q{INSERT INTO dbd_pg_test (id, pname, val) VALUES (2, 'Grapefruit', 'Pomegranate')}), $t);

$t='First database handle can see insert from second';
$rows = ($dbh->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 2}))[0];
cmp_ok ($rows, '==', 1, $t);

ok ($dbh->commit, 'Commit transaction with first database handle');

$t='Second database handle can see insert from first';
$rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
is ($rows, 1, $t);

ok ($dbh2->disconnect(), 'Disconnect with second database handle');

#
# Test of the database handle attribute "Driver"
#

$t='$dbh->{Driver}{Name} returns correct value of "Pg"';
$attrib = $dbh->{Driver}->{Name};
is ($attrib, 'Pg', $t);

#
# Test of the database handle attribute "Name"
#

SKIP: {

    $t='DB handle attribute "Name" returns same value as DBI_DSN';
    if (! length $testdsn or $testdsn !~ /^dbi:Pg:(.+)/) {
        skip (q{Cannot test DB handle attribute "Name" invalid DBI_DSN}, 1);
    }
    $expected = $1 || $ENV{PGDATABASE};
    defined $expected and length $expected or skip ('Cannot test unless database name known', 1);
    $attrib = $dbh->{Name};
    $expected =~ s/(db|database)=/dbname=/;
    is ($attrib, $expected, $t);
}


#
# Test of the database handle attribute "RowCacheSize"
#

$t='DB handle attribute "RowCacheSize" returns undef';
$attrib = $dbh->{RowCacheSize};
is ($attrib, undef, $t);

$t='Setting DB handle attribute "RowCacheSize" has no effect';
$dbh->{RowCacheSize} = 42;
$attrib = $dbh->{RowCacheSize};
is ($attrib, undef, $t);

#
# Test of the database handle attribute "Username"
#

$t='DB handle attribute "Username" returns the same value as DBI_USER';
$attrib = $dbh->{Username};
is ($attrib, $testuser, $t);

#
# Test of the "PrintWarn" database handle attribute
#

$t='DB handle attribute "PrintWarn" defaults to on';
my $value = $dbh->{PrintWarn};
is ($value, 1, $t);

{

local $SIG{__WARN__} = sub { $warning .= shift; };

$dbh->do(q{SET client_min_messages = 'DEBUG1'});
$t='DB handle attribute "PrintWarn" works when on';
$warning = q{};
eval {
    $dbh->do('CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)');
};
is ($@, q{}, $t);

$t='DB handle attribute "PrintWarn" shows warnings when on';
like ($warning, qr{dbd_pg_test_temp}, $t);


$t='DB handle attribute "PrintWarn" works when on';
$dbh->rollback();
$dbh->{PrintWarn}=0;
$warning = q{};
eval {
    $dbh->do('CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)');
};
is ($@, q{}, $t);

$t='DB handle attribute "PrintWarn" shows warnings when on';
is ($warning, q{}, $t);

$dbh->{PrintWarn}=1;
$dbh->rollback();

}

#
# Test of the database handle attributes "pg_INV_WRITE" and "pg_INV_READ"
# (these are used by the lo_* database handle methods)
#

$t='Database handle attribute "pg_INV_WRITE" returns a number';
like ($dbh->{pg_INV_WRITE}, qr/^\d+$/, $t);
$t='Database handle attribute "pg_INV_READ" returns a number';
like ($dbh->{pg_INV_READ}, qr/^\d+$/, $t);

#
# Test of the database handle attribute "pg_protocol"
#

$t='Database handle attribute "pg_protocol" returns a number';
like ($dbh->{pg_protocol}, qr/^\d+$/, $t);

#
# Test of the database handle attribute "pg_errorlevel"
#

$t='Database handle attribute "pg_errorlevel" returns the default (1)';
is ($dbh->{pg_errorlevel}, 1, $t);

$t='Database handle attribute "pg_errorlevel" defaults to 1 if invalid';
$dbh->{pg_errorlevel} = 3;
is ($dbh->{pg_errorlevel}, 1, $t);

#
# Test of the database handle attribute "pg_bool_tf"
#

$t='DB handle method "pg_bool_tf" starts as 0';
$result = $dbh->{pg_bool_tf}=0;
is ($result, 0, $t);

$t=q{DB handle method "pg_bool_tf" returns '1' for true when on};
$sth = $dbh->prepare('SELECT ?::bool');
$sth->bind_param(1,1,SQL_BOOLEAN);
$sth->execute();
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, '1', $t);

$t=q{DB handle method "pg_bool_tf" returns '0' for false when on};
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, '0', $t);

$t=q{DB handle method "pg_bool_tf" returns 't' for true when on};
$dbh->{pg_bool_tf}=1;
$sth->execute(1);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, 't', $t);

$t=q{DB handle method "pg_bool_tf" returns 'f' for true when on};
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is ($result, 'f', $t);


## Test of all the informational pg_* database handle attributes

$t='DB handle attribute "pg_db" returns at least one character';
$result = $dbh->{pg_protocol};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_db" returns at least one character';
$result = $dbh->{pg_db};
ok (length $result, $t);

$t='DB handle attribute "pg_user" returns a value';
$result = $dbh->{pg_user};
ok (defined $result, $t);

$t='DB handle attribute "pg_pass" returns a value';
$result = $dbh->{pg_pass};
ok (defined $result, $t);

$t='DB handle attribute "pg_port" returns a number';
$result = $dbh->{pg_port};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_default_port" returns a number';
$result = $dbh->{pg_default_port};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_options" returns a value';
$result = $dbh->{pg_options};
ok (defined $result, $t);

$t='DB handle attribute "pg_socket" returns a value';
$result = $dbh->{pg_socket};
like ($result, qr/^\d+$/, $t);

$t='DB handle attribute "pg_pid" returns a value';
$result = $dbh->{pg_pid};
like ($result, qr/^\d+$/, $t);

SKIP: {

    if ($pgversion < 80200) {
        skip ('Cannot test standard_conforming_strings on pre 8.2 servers', 3);
    }

    $t='DB handle attribute "pg_standard_conforming_strings" returns a valid value';
    my $oldscs = $dbh->{pg_standard_conforming_strings};
    like ($oldscs, qr/^on|off$/, $t);

    $t='DB handle attribute "pg_standard_conforming_strings" returns correct value';
    $dbh->do('SET standard_conforming_strings = on');
    $result = $dbh->{pg_standard_conforming_strings};
    is ($result, 'on', $t);

    $t='DB handle attribute "pg_standard_conforming_strings" returns correct value';
    $dbh->do('SET standard_conforming_strings = off');
    $result = $dbh->{pg_standard_conforming_strings};
    $dbh->do("SET standard_conforming_strings = $oldscs");
    is ($result, 'off', $t);
}

# Attempt to test whether or not we can get unicode out of the database
SKIP: {
    eval { require Encode; };
    skip ('Encode module is needed for unicode tests', 5) if $@;

    my $server_encoding = $dbh->selectall_arrayref('SHOW server_encoding')->[0][0];
    skip ('Cannot reliably test unicode without a UTF8 database', 5)
        if $server_encoding ne 'UTF8';

    $SQL = 'SELECT id, pname FROM dbd_pg_test WHERE id = ?';
    $sth = $dbh->prepare($SQL);
    $sth->execute(1);
    local $dbh->{pg_enable_utf8} = 1;

    $t='Quote method returns correct utf-8 characters';
    my $utf8_str = chr(0x100).'dam'; # LATIN CAPITAL LETTER A WITH MACRON
    is ($dbh->quote( $utf8_str ),  "'$utf8_str'", $t);

    $t='Able to insert unicode character into the database';
    $SQL = "INSERT INTO dbd_pg_test (id, pname, val) VALUES (40, '$utf8_str', 'Orange')";
    is ($dbh->do($SQL), '1', $t);

    $t='Able to read unicode (utf8) data from the database';
    $sth->execute(40);
    my ($id, $name) = $sth->fetchrow_array();
    ok (Encode::is_utf8($name), $t);

    $t='Unicode (utf8) data returned from database is not corrupted';
    is ($name, $utf8_str, $t);

    $t='ASCII text returned from database does have utf8 bit set';
    $sth->finish();
    $sth->execute(1);
    my ($id2, $name2) = $sth->fetchrow_array();
    ok (Encode::is_utf8($name2), $t);
    $sth->finish();
}

#
# Use the handle attribute "Warn" to check inheritance
#

undef $sth;

$t='Attribute "Warn" attribute set on by default';
ok ($dbh->{Warn}, $t);

$t='Statement handle inherits the "Warn" attribute';
$SQL = 'SELECT 123';
$sth = $dbh->prepare($SQL);
$sth->finish();
ok ($sth->{Warn}, $t);

$t='Able to turn off the "Warn" attribute in the database handle';
$dbh->{Warn} = 0;
ok (! $dbh->{Warn}, $t);

#
# Test of the the following statement handle attributes:
# NUM_OF_PARAMS, NUM_OF_FIELDS
# NAME, NAME_lc, NAME_uc, NAME_hash, NAME_lc_hash, NAME_uc_hash
# TYPE, PRECISION, SCALE, NULLABLE
#

## First, all pre-execute checks:

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly before execute with no placeholders';
$sth = $dbh->prepare('SELECT 123');
is ($sth->{'NUM_OF_PARAMS'}, 0, $t);

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly before execute with three placeholders';
$sth = $dbh->prepare('SELECT 123 FROM pg_class WHERE relname=? AND reltuples=? and relpages=?');
is ($sth->{'NUM_OF_PARAMS'}, 3, $t);

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly before execute with one placeholder';
$sth = $dbh->prepare('SELECT 123 AS "Sheep", CAST(id AS float) FROM dbd_pg_test WHERE id=?');
is ($sth->{'NUM_OF_PARAMS'}, 1, $t);

$t='Statement handle attribute "NUM_OF_FIELDS" returns undef before execute';
is ($sth->{'NUM_OF_FIELDS'}, undef, $t);

$t='Statement handle attribute "NAME" returns undef before execute';
is ($sth->{'NAME'}, undef, $t);

$t='Statement handle attribute "NAME_lc" returns undef before execute';
is ($sth->{'NAME_lc'}, undef, $t);

$t='Statement handle attribute "NAME_uc" returns undef before execute';
is ($sth->{'NAME_uc'}, undef, $t);

$t='Statement handle attribute "NAME_hash" returns undef before execute';
is ($sth->{'NAME_hash'}, undef, $t);

$t='Statement handle attribute "NAME_lc_hash" returns undef before execute';
is ($sth->{'NAME_lc_hash'}, undef, $t);

$t='Statement handle attribute "NAME_uc_hash" returns undef before execute';
is ($sth->{'NAME_uc_hash'}, undef, $t);

$t='Statement handle attribute "TYPE" returns undef before execute';
is ($sth->{'TYPE'}, undef, $t);

$t='Statement handle attribute "PRECISION" returns undef before execute';
is ($sth->{'PRECISION'}, undef, $t);

$t='Statement handle attribute "SCALE" returns undef before execute';
is ($sth->{'SCALE'}, undef, $t);

$t='Statement handle attribute "NULLABLE" returns undef before execute';
is ($sth->{'NULLABLE'}, undef, $t);

## Now, some post-execute checks:

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly after execute';
$sth->execute(12);
is ($sth->{'NUM_OF_PARAMS'}, 1, $t);

$t='Statement handle attribute "NUM_OF_FIELDS" works correctly for SELECT statements';
is ($sth->{'NUM_OF_FIELDS'}, 2, $t);

$t='Statement handle attribute "NAME" works correctly for SELECT statements';
my $colnames = ['Sheep', 'id'];
is_deeply ($sth->{'NAME'}, $colnames, $t);

$t='Statement handle attribute "NAME_lc" works correctly for SELECT statements';
$colnames = ['sheep', 'id'];
is_deeply ($sth->{'NAME_lc'}, $colnames, $t);

$t='Statement handle attribute "NAME_uc" works correctly for SELECT statements';
$colnames = ['SHEEP', 'ID'];
is_deeply ($sth->{'NAME_uc'}, $colnames, $t);

$t='Statement handle attribute "NAME_hash" works correctly for SELECT statements';
$colnames = {'Sheep' => 0, id => 1};
is_deeply ($sth->{'NAME_hash'}, $colnames, $t);

$t='Statement handle attribute "NAME_lc_hash" works correctly for SELECT statements';
$colnames = {'sheep' => 0, id => 1};
is_deeply ($sth->{'NAME_lc_hash'}, $colnames, $t);

$t='Statement handle attribute "NAME_uc_hash" works correctly for SELECT statements';
$colnames = {'SHEEP' => 0, ID => 1};
is_deeply ($sth->{'NAME_uc_hash'}, $colnames, $t);

$t='Statement handle attribute "TYPE" works correctly for SELECT statements';
$colnames = [4, 6];
is_deeply ($sth->{'TYPE'}, $colnames, $t);

$t='Statement handle attribute "PRECISION" works correctly';
$colnames = [4, 8];
is_deeply ($sth->{'PRECISION'}, $colnames, $t);

$t='Statement handle attribute "SCALE" works correctly';
$colnames = [undef,undef];
is_deeply ($sth->{'SCALE'}, $colnames, $t);

$t='Statement handle attribute "NULLABLE" works correctly';
$colnames = [2,2];
is_deeply ($sth->{NULLABLE}, $colnames, $t);

## Post-finish tasks:

$sth->finish();

$t='Statement handle attribute "NUM_OF_PARAMS" works correctly after finish';
is ($sth->{'NUM_OF_PARAMS'}, 1, $t);

$t='Statement handle attribute "NUM_OF_FIELDS" works correctly after finish';
is ($sth->{'NUM_OF_FIELDS'}, 2, $t);

$t='Statement handle attribute "NAME" returns undef after finish';
is_deeply ($sth->{'NAME'}, undef, $t);

$t='Statement handle attribute "NAME_lc" returns values after finish';
$colnames = ['sheep', 'id'];
is_deeply ($sth->{'NAME_lc'}, $colnames, $t);

$t='Statement handle attribute "NAME_uc" returns values after finish';
$colnames = ['SHEEP', 'ID'];
is_deeply ($sth->{'NAME_uc'}, $colnames, $t);

$t='Statement handle attribute "NAME_hash" works correctly after finish';
$colnames = {'Sheep' => 0, id => 1};
is_deeply ($sth->{'NAME_hash'}, $colnames, $t);

$t='Statement handle attribute "NAME_lc_hash" works correctly after finish';
$colnames = {'sheep' => 0, id => 1};
is_deeply ($sth->{'NAME_lc_hash'}, $colnames, $t);

$t='Statement handle attribute "NAME_uc_hash" works correctly after finish';
$colnames = {'SHEEP' => 0, ID => 1};
is_deeply ($sth->{'NAME_uc_hash'}, $colnames, $t);

$t='Statement handle attribute "TYPE" returns undef after finish';
is_deeply ($sth->{'TYPE'}, undef, $t);

$t='Statement handle attribute "PRECISION" works correctly after finish';
is_deeply ($sth->{'PRECISION'}, undef, $t);

$t='Statement handle attribute "SCALE" works correctly after finish';
is_deeply ($sth->{'SCALE'}, undef, $t);

$t='Statement handle attribute "NULLABLE" works correctly after finish';
is_deeply ($sth->{NULLABLE}, undef, $t);

## Test UPDATE queries

$t='Statement handle attribute "NUM_OF_FIELDS" returns undef for updates';
$sth = $dbh->prepare('UPDATE dbd_pg_test SET id = 99 WHERE id = ?');
$sth->execute(1);
is_deeply ($sth->{'NUM_OF_FIELDS'}, undef, $t);

$t='Statement handle attribute "NAME" returns empty arrayref for updates';
is_deeply ($sth->{'NAME'}, [], $t);

## These cause assertion errors, may be a DBI bug.
## Commenting out for now until we can examine closer
## Please see: http://www.nntp.perl.org/group/perl.cpan.testers/2008/08/msg2012293.html

#$t='Statement handle attribute "NAME_lc" returns empty arrayref for updates';
#is_deeply ($sth->{'NAME_lc'}, [], $t);

#$t='Statement handle attribute "NAME_uc" returns empty arrayref for updates';
#is_deeply ($sth->{'NAME_uc'}, [], $t);

#$t='Statement handle attribute "NAME_hash" returns empty hashref for updates';
#is_deeply ($sth->{'NAME_hash'}, {}, $t);

#$t='Statement handle attribute "NAME_uc_hash" returns empty hashref for updates';
#is_deeply ($sth->{'NAME_lc_hash'}, {}, $t);

#$t='Statement handle attribute "NAME_uc_hash" returns empty hashref for updates';
#is_deeply ($sth->{'NAME_uc_hash'}, {}, $t);

$t='Statement handle attribute "TYPE" returns empty arrayref for updates';
is_deeply ($sth->{'TYPE'}, [], $t);

$t='Statement handle attribute "PRECISION" returns empty arrayref for updates';
is_deeply ($sth->{'PRECISION'}, [], $t);

$t='Statement handle attribute "SCALE" returns empty arrayref for updates';
is_deeply ($sth->{'SCALE'}, [], $t);

$t='Statement handle attribute "NULLABLE" returns empty arrayref for updates';
is_deeply ($sth->{'NULLABLE'}, [], $t);

$dbh->do('UPDATE dbd_pg_test SET id = 1 WHERE id = 99');

## Test UPDATE,INSERT, and DELETE with RETURNING

SKIP: {

    if ($pgversion < 80200) {
        skip ('Cannot test RETURNING clause on pre 8.2 servers', 33);
    }

    $t='Statement handle attribute "NUM_OF_FIELDS" returns correct value for RETURNING updates';
    $sth = $dbh->prepare('UPDATE dbd_pg_test SET id = 99 WHERE id = ? RETURNING id, expo, "CaseTest"');
    $sth->execute(1);
    is_deeply ($sth->{'NUM_OF_FIELDS'}, 3, $t);

    $t='Statement handle attribute "NAME" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME'}, ['id','expo','CaseTest'], $t);

    $t='Statement handle attribute "NAME_lc" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME_lc'}, ['id','expo','casetest'], $t);

    $t='Statement handle attribute "NAME_uc" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME_uc'}, ['ID','EXPO','CASETEST'], $t);

    $t='Statement handle attribute "NAME_hash" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME_hash'}, {id=>0, expo=>1, CaseTest=>2}, $t);

    $t='Statement handle attribute "NAME_lc_hash" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME_lc_hash'}, {id=>0, expo=>1, casetest=>2}, $t);

    $t='Statement handle attribute "NAME_uc_hash" returns correct info for RETURNING updates';
    is_deeply ($sth->{'NAME_uc_hash'}, {ID=>0, EXPO=>1, CASETEST=>2}, $t);

    $t='Statement handle attribute "TYPE" returns correct info for RETURNING updates';
    is_deeply ($sth->{'TYPE'}, [4,2,16], $t);

    $t='Statement handle attribute "PRECISION" returns correct info for RETURNING updates';
    is_deeply ($sth->{'PRECISION'}, [4,6,1], $t);

    $t='Statement handle attribute "SCALE" returns correct info for RETURNING updates';
    is_deeply ($sth->{'SCALE'}, [undef,2,undef], $t);

    $t='Statement handle attribute "NULLABLE" returns empty arrayref for updates';
    is_deeply ($sth->{'NULLABLE'}, [0,1,1], $t);

    $dbh->do('UPDATE dbd_pg_test SET id = 1 WHERE id = 99');

    $t='Statement handle attribute "NUM_OF_FIELDS" returns correct value for RETURNING inserts';
    $sth = $dbh->prepare('INSERT INTO dbd_pg_test(id) VALUES(?) RETURNING id, lii, expo, "CaseTest"');
    $sth->execute(88);
    is_deeply ($sth->{'NUM_OF_FIELDS'}, 4, $t);

    $t='Statement handle attribute "NAME" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME'}, ['id','lii','expo','CaseTest'], $t);

    $t='Statement handle attribute "NAME_lc" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME_lc'}, ['id','lii','expo','casetest'], $t);

    $t='Statement handle attribute "NAME_uc" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME_uc'}, ['ID','LII','EXPO','CASETEST'], $t);

    $t='Statement handle attribute "NAME_hash" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME_hash'}, {id=>0, lii=>1, expo=>2, CaseTest=>3}, $t);

    $t='Statement handle attribute "NAME_lc_hash" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME_lc_hash'}, {id=>0, lii=>1, expo=>2, casetest=>3}, $t);

    $t='Statement handle attribute "NAME_uc_hash" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'NAME_uc_hash'}, {ID=>0, LII=>1, EXPO=>2, CASETEST=>3}, $t);

    $t='Statement handle attribute "TYPE" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'TYPE'}, [4,4,2,16], $t);

    $t='Statement handle attribute "PRECISION" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'PRECISION'}, [4,4,6,1], $t);

    $t='Statement handle attribute "SCALE" returns correct info for RETURNING inserts';
    is_deeply ($sth->{'SCALE'}, [undef,undef,2,undef], $t);

    $t='Statement handle attribute "NULLABLE" returns empty arrayref for inserts';
    is_deeply ($sth->{'NULLABLE'}, [0,0,1,1], $t);

    $t='Statement handle attribute "NUM_OF_FIELDS" returns correct value for RETURNING updates';
    $sth = $dbh->prepare('DELETE FROM dbd_pg_test WHERE id = 88 RETURNING id, lii, expo, "CaseTest"');
    $sth->execute();
    is_deeply ($sth->{'NUM_OF_FIELDS'}, 4, $t);

    $t='Statement handle attribute "NAME" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME'}, ['id','lii','expo','CaseTest'], $t);

    $t='Statement handle attribute "NAME_lc" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME_lc'}, ['id','lii','expo','casetest'], $t);

    $t='Statement handle attribute "NAME_uc" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME_uc'}, ['ID','LII','EXPO','CASETEST'], $t);

    $t='Statement handle attribute "NAME_hash" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME_hash'}, {id=>0, lii=>1, expo=>2, CaseTest=>3}, $t);

    $t='Statement handle attribute "NAME_lc_hash" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME_lc_hash'}, {id=>0, lii=>1, expo=>2, casetest=>3}, $t);

    $t='Statement handle attribute "NAME_uc_hash" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'NAME_uc_hash'}, {ID=>0, LII=>1, EXPO=>2, CASETEST=>3}, $t);

    $t='Statement handle attribute "TYPE" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'TYPE'}, [4,4,2,16], $t);

    $t='Statement handle attribute "PRECISION" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'PRECISION'}, [4,4,6,1], $t);

    $t='Statement handle attribute "SCALE" returns correct info for RETURNING deletes';
    is_deeply ($sth->{'SCALE'}, [undef,undef,2,undef], $t);

    $t='Statement handle attribute "NULLABLE" returns empty arrayref for deletes';
    is_deeply ($sth->{'NULLABLE'}, [0,0,1,1], $t);

}

$t='Statement handle attribute "NUM_OF_FIELDS" returns correct value for SHOW commands';
$sth = $dbh->prepare('SHOW random_page_cost');
$sth->execute();
is_deeply ($sth->{'NUM_OF_FIELDS'}, 1, $t);

$t='Statement handle attribute "NAME" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME'}, ['random_page_cost'], $t);

$t='Statement handle attribute "NAME_lc" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME_lc'}, ['random_page_cost'], $t);

$t='Statement handle attribute "NAME_uc" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME_uc'}, ['RANDOM_PAGE_COST'], $t);

$t='Statement handle attribute "NAME_hash" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME_hash'}, {random_page_cost=>0}, $t);

$t='Statement handle attribute "NAME_lc_hash" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME_lc_hash'}, {random_page_cost=>0}, $t);

$t='Statement handle attribute "NAME_uc_hash" returns correct info for SHOW commands';
is_deeply ($sth->{'NAME_uc_hash'}, {RANDOM_PAGE_COST=>0}, $t);

$t='Statement handle attribute "TYPE" returns correct info for SHOW commands';
is_deeply ($sth->{'TYPE'}, [-1], $t);

$t='Statement handle attribute "PRECISION" returns correct info for SHOW commands';
is_deeply ($sth->{'PRECISION'}, [undef], $t);

$t='Statement handle attribute "SCALE" returns correct info for SHOW commands';
is_deeply ($sth->{'SCALE'}, [undef], $t);

$t='Statement handle attribute "NULLABLE" returns "unknown" (2) for SHOW commands';
is_deeply ($sth->{'NULLABLE'}, [2], $t);


#
# Test of the statement handle attribute "CursorName"
#

$t='Statement handle attribute "CursorName" returns undef';
$attrib = $sth->{CursorName};
is ($attrib, undef, $t);

#
# Test of the statement handle attribute "Database"
#

$t='Statement handle attribute "Database" matches the database handle';
$attrib = $sth->{Database};
is ($attrib, $dbh, $t);

#
# Test of the statement handle attribute "ParamValues"
#

$t='Statement handle attribute "ParamValues" works before execute';
$sth = $dbh->prepare('SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND pname=?');
$sth->bind_param(1, 99);
$sth->bind_param(2, undef);
$sth->bind_param(3, 'Sparky');
$attrib = $sth->{ParamValues};
$expected = {1 => '99', 2 => undef, 3 => 'Sparky'};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attribute "ParamValues" works after execute';
$sth->execute();
$attrib = $sth->{ParamValues};
is_deeply ($attrib, $expected, $t);

#
# Test of the statement handle attribute "ParamTypes"
#


$t='Statement handle attribute "ParamTypes" works before execute';
$sth = $dbh->prepare('SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND lii=?');
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->bind_param(2, 'TMW', SQL_VARCHAR);
$attrib = $sth->{ParamTypes};
$expected = {1 => {TYPE => SQL_INTEGER}, 2 => {TYPE => SQL_VARCHAR}, 3 => undef};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attributes "ParamValues" and "ParamTypes" can be passed back to bind_param';
eval {
    my $vals = $sth->{ParamValues};
    my $types = $sth->{ParamTypes};
    $sth->bind_param($_, $vals->{$_}, $types->{$_} )
        for keys %$types;
};
is( $@, q{}, $t);

$t='Statement handle attribute "ParamTypes" works before execute with named placeholders';
$sth = $dbh->prepare('SELECT id FROM dbd_pg_test WHERE id=:foobar AND val=:foobar2 AND lii=:foobar3');
$sth->bind_param(':foobar', 1, {pg_type => PG_INT4});
$sth->bind_param(':foobar2', 'TMW', {pg_type => PG_TEXT});
$attrib = $sth->{ParamTypes};
$expected = {':foobar' => {TYPE => SQL_INTEGER}, ':foobar2' => {TYPE => SQL_LONGVARCHAR}, ':foobar3' => undef};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attributes "ParamValues" and "ParamTypes" can be passed back to bind_param';
eval {
    my $vals = $sth->{ParamValues};
    my $types = $sth->{ParamTypes};
    $sth->bind_param($_, $vals->{$_}, $types->{$_} )
        for keys %$types;
};
is( $@, q{}, $t);

$t='Statement handle attribute "ParamTypes" works after execute';
$sth->bind_param(':foobar3', 3, {pg_type => PG_INT2});
$sth->execute();
$attrib = $sth->{ParamTypes};
$expected->{':foobar3'} = {TYPE => SQL_SMALLINT};
is_deeply ($attrib, $expected, $t);

$t='Statement handle attribute "ParamTypes" returns correct values';
$sth->bind_param(':foobar2', 3, {pg_type => PG_CIRCLE});
$attrib = $sth->{ParamTypes}{':foobar2'};
$expected = {pg_type => PG_CIRCLE};
is_deeply ($attrib, $expected, $t);

#
# Test of the statement handle attribute "RowsInCache"
#

$t='Statement handle attribute "RowsInCache" returns undef';
$attrib = $sth->{RowsInCache};
is ($attrib, undef, $t);


#
# Test of the statement handle attribute "pg_size"
#

$t='Statement handle attribute "pg_size" works';
$SQL = q{SELECT id, pname, val, score, Fixed, pdate, "CaseTest" FROM dbd_pg_test};
$sth = $dbh->prepare($SQL);
$sth->execute();
$result = $sth->{pg_size};
$expected = [qw(4 -1 -1 8 -1 8 1)];
is_deeply ($result, $expected, $t);

#
# Test of the statement handle attribute "pg_type"
#

$t='Statement handle attribute "pg_type" works';
$sth->execute();
$result = $sth->{pg_type};
$expected = [qw(int4 varchar text float8 bpchar timestamp bool)];
is_deeply ($result, $expected, $t);
$sth->finish();

#
# Test of the statement handle attribute "pg_oid_status"
#

$t='Statement handle attribute "pg_oid_status" returned a numeric value after insert';
$SQL = q{INSERT INTO dbd_pg_test (id, val) VALUES (?, 'lemon')};
$sth = $dbh->prepare($SQL);
$sth->bind_param('$1','',SQL_INTEGER);
$sth->execute(500);
$result = $sth->{pg_oid_status};
like ($result, qr/^\d+$/, $t);

#
# Test of the statement handle attribute "pg_cmd_status"
#

## INSERT DELETE UPDATE SELECT
for (
q{INSERT INTO dbd_pg_test (id,val) VALUES (400, 'lime')},
q{DELETE FROM dbd_pg_test WHERE id=1},
q{UPDATE dbd_pg_test SET id=2 WHERE id=2},
q{SELECT * FROM dbd_pg_test},
    ) {
    $expected = substr($_,0,6);
    $t=qq{Statement handle attribute "pg_cmd_status" works for '$expected'};
    $sth = $dbh->prepare($_);
    $sth->execute();
    $result = $sth->{pg_cmd_status};
    $sth->finish();
    like ($result, qr/^$expected/, $t);
}

#
# Test of the datbase and statement handle attribute "pg_async_status"
#

$t=q{Statement handle attribute "pg_async_status" returns a 0 as default value};
is ($sth->{pg_async_status}, 0, $t);
$t=q{Database handle attribute "pg_async_status" returns a 0 as default value};
is ($dbh->{pg_async_status}, 0, $t);

$t=q{Statement handle attribute "pg_async_status" returns a 0 after a normal prepare};
$sth = $dbh->prepare('SELECT 123');
is ($sth->{pg_async_status}, 0, $t);
$t=q{Database handle attribute "pg_async_status" returns a 0 after a normal prepare};
is ($dbh->{pg_async_status}, 0, $t);

$t=q{Statement handle attribute "pg_async_status" returns a 0 after a normal execute};
$sth->execute();
is ($sth->{pg_async_status}, 0, $t);
$t=q{Database handle attribute "pg_async_status" returns a 0 after a normal execute};
is ($sth->{pg_async_status}, 0, $t);

$t=q{Statement handle attribute "pg_async_status" returns a 0 after an asynchronous prepare};
$sth = $dbh->prepare('SELECT 123', { pg_async => PG_ASYNC });
is ($sth->{pg_async_status}, 0, $t);
$t=q{Database handle attribute "pg_async_status" returns a 0 after an asynchronous prepare};
is ($dbh->{pg_async_status}, 0, $t);
$sth->execute();
$t=q{Statement handle attribute "pg_async_status" returns a 1 after an asynchronous execute};
is ($sth->{pg_async_status}, 1, $t);
$t=q{Database handle attribute "pg_async_status" returns a 1 after an asynchronous execute};
is ($dbh->{pg_async_status}, 1, $t);

$t=q{Statement handle attribute "pg_async_status" returns a -1 after a cancel};
$dbh->pg_cancel();
is ($sth->{pg_async_status}, -1, $t);
$t=q{Database handle attribute "pg_async_status" returns a -1 after a cancel};
is ($dbh->{pg_async_status}, -1, $t);

#
# Test of the handle attribute "Active"
#


$t='Database handle attribute "Active" is true while connected';
$attrib = $dbh->{Active};
is ($attrib, 1, $t);


$sth = $dbh->prepare('SELECT 123 UNION SELECT 456');
$attrib = $sth->{Active};
is ($attrib, '', $t);

$t='Statement handle attribute "Active" is true after SELECT';
$sth->execute();
$attrib = $sth->{Active};
is ($attrib, 1, $t);

$t='Statement handle attribute "Active" is true when rows remaining';
my $row = $sth->fetchrow_arrayref();
$attrib = $sth->{Active};
is ($attrib, 1, $t);

$t='Statement handle attribute "Active" is false after finish called';
$sth->finish();
$attrib = $sth->{Active};
is ($attrib, '', $t);

#
# Test of the handle attribute "Executed"
#


my $dbh3 = connect_database({quickreturn => 1});
$dbh3->{AutoCommit} = 0;

$t='Database handle attribute "Executed" begins false';
is ($dbh3->{Executed}, '', $t);

$t='Database handle attribute "Executed" stays false after prepare()';
$sth = $dbh3->prepare('SELECT 12345');
is ($dbh3->{Executed}, '', $t);

$t='Statement handle attribute "Executed" begins false';
is ($sth->{Executed}, '', $t);

$t='Statement handle attribute "Executed" is true after execute()';
$sth->execute();
is ($sth->{Executed}, 1, $t);

$t='Database handle attribute "Executed" is true after execute()';
is ($dbh3->{Executed}, 1, $t);

$t='Statement handle attribute "Executed" is true after finish()';
$sth->finish();
is ($sth->{Executed}, 1, $t);

$t='Database handle attribute "Executed" is true after finish()';
is ($dbh3->{Executed}, 1, $t);

$t='Database handle attribute "Executed" is false after commit()';
$dbh3->commit();
is ($dbh3->{Executed}, '', $t);

$t='Statement handle attribute "Executed" is true after commit()';
is ($sth->{Executed}, 1, $t);

$t='Database handle attribute "Executed" is true after do()';
$dbh3->do('SELECT 1234');
is ($dbh3->{Executed}, 1, $t);

$t='Database handle attribute "Executed" is false after rollback()';
$dbh3->commit();
is ($dbh3->{Executed}, '', $t);

$t='Statement handle attribute "Executed" is true after rollback()';
is ($sth->{Executed}, 1, $t);

#
# Test of the handle attribute "Kids"
#

$t='Database handle attribute "Kids" is set properly';
$attrib = $dbh3->{Kids};
is ($attrib, 1, $t);

$t='Database handle attribute "Kids" works';
my $sth2 = $dbh3->prepare('SELECT 234');
$attrib = $dbh3->{Kids};
is ($attrib, 2, $t);

$t='Statement handle attribute "Kids" is zero';
$attrib = $sth2->{Kids};
is ($attrib, 0, $t);

#
# Test of the handle attribute "ActiveKids"
#

$t='Database handle attribute "ActiveKids" is set properly';
$attrib = $dbh3->{ActiveKids};
is ($attrib, 0, $t);

$t='Database handle attribute "ActiveKids" works';
$sth2 = $dbh3->prepare('SELECT 234');
$sth2->execute();
$attrib = $dbh3->{ActiveKids};
is ($attrib, 1, $t);

$t='Statement handle attribute "ActiveKids" is zero';
$attrib = $sth2->{ActiveKids};
is ($attrib, 0, $t);
$sth2->finish();

#
# Test of the handle attribute "CachedKids"
#

$t='Database handle attribute "CachedKids" is set properly';
$attrib = $dbh3->{CachedKids};
is (keys %$attrib, 0, $t);
my $sth4 = $dbh3->prepare_cached('select 1');
$attrib = $dbh3->{CachedKids};
is (keys %$attrib, 1, $t);
$sth4->finish();

$dbh3->disconnect();

#
# Test of the handle attribute "Type"
#

$t='Database handle attribute "Type" is set properly';
$attrib = $dbh->{Type};
is ($attrib, 'db', $t);

$t='Statement handle attribute "Type" is set properly';
$sth = $dbh->prepare('SELECT 1');
$attrib = $sth->{Type};
is ($attrib, 'st', $t);

#
# Test of the handle attribute "ChildHandles"
# Need a separate connection to keep the output size down
#

my $dbh4 = connect_database({quickreturn => 2});

$t='Database handle attribute "ChildHandles" is an empty list on startup';
$attrib = $dbh4->{ChildHandles};
is_deeply ($attrib, [], $t);

$t='Statement handle attribute "ChildHandles" is an empty list on creation';
{
    my $sth5 = $dbh4->prepare('SELECT 1');
    $attrib = $sth5->{ChildHandles};
    is_deeply ($attrib, [], $t);

    $t='Database handle attribute "ChildHandles" contains newly created statement handle';
    $attrib = $dbh4->{ChildHandles};
    is_deeply ($attrib, [$sth5], $t);

    $sth4->finish();

} ## sth5 now out of scope

$t='Database handle attribute "ChildHandles" has undef for destroyed statement handle';
$attrib = $dbh4->{ChildHandles};
is_deeply ($attrib, [undef], $t);

$dbh4->disconnect();

#
# Test of the handle attribute "CompatMode"
#

$t='Database handle attribute "CompatMode" is set properly';
$attrib = $dbh->{CompatMode};
ok (!$attrib, $t);

#
# Test of the handle attribute PrintError
#

$t='Database handle attribute "PrintError" is set properly';
$attrib = $dbh->{PrintError};
is ($attrib, '', $t);

# Make sure that warnings are sent back to the client
$SQL = 'Testing the DBD::Pg modules error handling -?-';
$dbh->do(q{SET client_min_messages = 'NOTICE'});

$warning = '';
local $SIG{__WARN__} = sub { $warning = shift; };
$dbh->{RaiseError} = 0;

$t='Warning thrown when database handle attribute "PrintError" is on';
$dbh->{PrintError} = 1;
$sth = $dbh->prepare($SQL);
$sth->execute();
isnt ($warning, undef, $t);

$t='No warning thrown when database handle attribute "PrintError" is off';
undef $warning;
$dbh->{PrintError} = 0;
$sth = $dbh->prepare($SQL);
$sth->execute();
is ($warning, undef, $t);

## Special case in which errors are not sent to the client!
SKIP: {
    $t = q{When client_min_messages is FATAL, we do our best to alert the caller it's a Bad Idea};
    $dbh->do(q{SET client_min_messages = 'FATAL'});
    skip 'This version of PostgreSQL caps client_min_messages to ERROR', 1
        unless $dbh->selectrow_array('SHOW client_min_messages') eq 'fatal';

    $dbh->{RaiseError} = 0;
    $dbh->{AutoCommit} = 1;
    eval {
        $dbh->do('SELECT 1 FROM nonesuh');
    };
    my $errorstring = $dbh->errstr;
    like ($errorstring, qr/Perhaps client_min_messages/, $t);
}
$dbh->rollback();
$dbh->do(q{SET client_min_message = 'NOTICE'});
$dbh->{RaiseError} = 1;
$dbh->{AutoCommit} = 0;


#
# Test of the handle attribute RaiseError
#

$t='No error produced when database handle attribute "RaiseError" is off';
$dbh->{RaiseError} = 0;
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute();
};
is ($@, q{}, $t);

$t='Error produced when database handle attribute "RaiseError" is off';
$dbh->{RaiseError} = 1;
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute();
};
isnt ($@, q{}, $t);


#
# Test of the handle attribute HandleError
#

$t='Database handle attribute "HandleError" is set properly';
$attrib = $dbh->{HandleError};
ok (!$attrib, $t);

$t='Database handle attribute "HandleError" works';
undef $warning;
$dbh->{HandleError} = sub { $warning = shift; };
$sth = $dbh->prepare($SQL);
$sth->execute();
ok ($warning, $t);

$t='Database handle attribute "HandleError" modifies error messages';
undef $warning;
$dbh->{HandleError} = sub { $_[0] = "Slonik $_[0]"; 0; };
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute();
};
like ($@, qr/^Slonik/, $t);
$dbh->{HandleError}= undef;
$dbh->rollback();

#
# Test of the handle attribute HandleSetErr
#

$t='Database handle attribute "HandleSetErr" is set properly';
$attrib = $dbh->{HandleSetErr};
ok (!$attrib, $t);

$t='Database handle attribute "HandleSetErr" works as expected';
undef $warning;
$dbh->{HandleSetErr} = sub {
    my ($h,$err,$errstr,$state,$method) = @_;
    $_[1] = 42;
    $_[2] = 'ERRSTR';
    $_[3] = '33133';
    return;
};
eval {$sth = $dbh->last_insert_id('cat', 'schema', 'table', 'col', ['notahashref']); };
## Changing the state does not work yet.
like ($@, qr{ERRSTR}, $t);
is ($dbh->errstr, 'ERRSTR', $t);
is ($dbh->err, '42', $t);
$dbh->{HandleSetErr} = 0;
$dbh->rollback();

#
# Test of the handle attribute "ErrCount"
#

$t='Database handle attribute "ErrCount" starts out at 0';
$dbh4 = connect_database({quickreturn => 2});
is ($dbh4->{ErrCount}, 0, $t);

$t='Database handle attribute "ErrCount" is incremented with set_err()';
eval {$sth = $dbh4->last_insert_id('cat', 'schema', 'table', 'col', ['notahashref']); };
is ($dbh4->{ErrCount}, 1, $t);

$dbh4->disconnect();

#
# Test of the handle attribute "ShowErrorStatement"
#

$t='Database handle attribute "ShowErrorStatemnt" starts out false';
is ($dbh->{ShowErrorStatement}, '', $t);

$t='Database handle attribute "ShowErrorStatement" has no effect if not set';
$SQL = 'Testing the ShowErrorStatement attribute';
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute();
};
unlike ($@, qr{for Statement "Testing}, $t);

$t='Database handle attribute "ShowErrorStatement" adds statement to errors';
$dbh->{ShowErrorStatement} = 1;
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute();
};
like ($@, qr{for Statement "Testing}, $t);

$t='Database handle attribute "ShowErrorStatement" adds statement and placeholders to errors via execute() with null args';
$SQL = q{SELECT 'Another ShowErrorStatement Test' FROM pg_class WHERE relname = ? AND reltuples = ?};
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(123);
};
like ($@, qr{with ParamValues}, $t);

$t='Statement handle attribute "ShowErrorStatement" adds statement and placeholders to errors via execute()';
$SQL = q{SELECT 'Another ShowErrorStatement Test' FROM pg_class WHERE relname = ? AND reltuples = ?};
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(123,456);
};
like ($@, qr{with ParamValues: 1='123', 2='456'}, $t);

$t='Database handle attribute "ShowErrorStatement" adds statement and placeholders to errors via do()';
$SQL = q{SELECT 'Another ShowErrorStatement Test' FROM pg_class WHERE relname = ? AND reltuples = ?};
eval {
    $dbh->do($SQL, {}, 123, 456);
};
like ($@, qr{with ParamValues: 1='123', 2='456'}, $t);

$dbh->commit();

#
# Test of the handle attribute TraceLevel
#

$t='Database handle attribute "TraceLevel" returns a number';
$attrib = $dbh->{TraceLevel};
like ($attrib, qr/^\d$/, $t);

#
# Test of the handle attribute FetchHashKeyName
#

# The default is mixed case ("NAME");
$t='Database handle attribute "FetchHashKeyName" is set properly';
$attrib = $dbh->{FetchHashKeyName};
is ($attrib, 'NAME', $t);

$t='Database handle attribute "FetchHashKeyName" works with the default value of NAME';
$SQL = q{SELECT "CaseTest" FROM dbd_pg_test};
$sth = $dbh->prepare($SQL);
$sth->execute();
my ($colname) = keys %{$sth->fetchrow_hashref()};
$sth->finish();
is ($colname, 'CaseTest', $t);

$t='Database handle attribute "FetchHashKeyName" can be changed';
$dbh->{FetchHashKeyName} = 'NAME_lc';
$attrib = $dbh->{FetchHashKeyName};
is ($attrib, 'NAME_lc', $t);

$t='Database handle attribute "FetchHashKeyName" works with a value of NAME_lc';
$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
is ($colname, 'casetest', $t);
$sth->finish();

$t='Database handle attribute "FetchHashKeyName" works with a value of NAME_uc';
$dbh->{FetchHashKeyName} = 'NAME_uc';
$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
$sth->finish();
$dbh->{FetchHashKeyName} = 'NAME';
is ($colname, 'CASETEST', $t);

#
# Test of the handle attribute ChopBlanks
#


$t='Database handle attribute "ChopBlanks" is set properly';
$attrib = $dbh->{ChopBlanks};
ok (!$attrib, $t);

$dbh->do('DELETE FROM dbd_pg_test');
$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed, val) VALUES (3, ' Fig', ' Raspberry ')});

$t='Database handle attribute "ChopBlanks" = 0 returns correct value for fixed-length column';
$dbh->{ChopBlanks} = 0;
my ($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is ($val, ' Fig ', $t);

$t='Database handle attribute "ChopBlanks" = 0 returns correct value for variable-length column';
($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
is ($val, ' Raspberry ', $t);

$t='Database handle attribute "ChopBlanks" = 1 returns correct value for fixed-length column';
$dbh->{ChopBlanks}=1;
($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is ($val, ' Fig', $t);

$t='Database handle attribute "ChopBlanks" = 1 returns correct value for variable-length column';
($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
$dbh->do('DELETE from dbd_pg_test');
is ($val, ' Raspberry ', $t);

#
# Test of the handle attribute LongReadLen
#

$t='Handle attribute "LongReadLen" has been set properly';
$attrib = $dbh->{LongReadLen};
ok ($attrib, $t);

#
# Test of the handle attribute LongTruncOk
#

$t='Handle attribute "LongTruncOk" has been set properly';
$attrib = $dbh->{LongTruncOk};
ok (!$attrib, $t);

#
# Test of the handle attribute TaintIn
#

$t='Handle attribute "TaintIn" has been set properly';
$attrib = $dbh->{TaintIn};
is ($attrib, '', $t);

#
# Test of the handle attribute TaintOut
#

$t='Handle attribute "TaintOut" has been set properly';
$attrib = $dbh->{TaintOut};
is ($attrib, '', $t);

#
# Test of the handle attribute Taint
#

$t='Handle attribute "Taint" has been set properly';
$attrib = $dbh->{Taint};
is ($attrib, '', $t);

$t='The value of handle attribute "Taint" can be changed';
$dbh->{Taint}=1;
$attrib = $dbh->{Taint};
is ($attrib, 1, $t);

$t='Changing handle attribute "Taint" changes "TaintIn"';
$attrib = $dbh->{TaintIn};
is ($attrib, 1, $t);

$t='Changing handle attribute "Taint" changes "TaintOut"';
$attrib = $dbh->{TaintOut};
is ($attrib, 1, $t);

#
# Not tested: handle attribute Profile
#

#
# Test of the database handle attribute "ReadOnly"
#

SKIP: {
    if ($DBI::VERSION < 1.55) {
        skip ('DBI must be at least version 1.55 to test DB attribute "ReadOnly"', 8);
    }

    $t='Database handle attribute "ReadOnly" starts out undefined';
    $dbh->commit();

    ## This fails on some boxes, so we pull back all information to display why
    my ($helpconnect2, $connerror2);
    ($helpconnect2, $connerror2, $dbh4) = connect_database();
    if (! defined $dbh4) {
        die "Database connection failed: helpconnect is $helpconnect2, error is $connerror2\n";
    }
    $dbh4->trace(0);
    is ($dbh4->{ReadOnly}, undef, $t);

    $t='Database handle attribute "ReadOnly" allows SELECT queries to work when on';
    $dbh4->{ReadOnly} = 1;
    $result = $dbh4->selectall_arrayref('SELECT 12345')->[0][0];
    is ($result, 12345, $t);

    $t='Database handle attribute "ReadOnly" prevents INSERT queries from working when on';
    $SQL = 'INSERT INTO dbd_pg_test (id) VALUES (50)';
    eval { $dbh4->do($SQL); };
    is($dbh4->state, '25006', $t);
    $dbh4->rollback();

    $sth = $dbh4->prepare($SQL);
    eval { $sth->execute(); };
    is($dbh4->state, '25006', $t);
    $dbh4->rollback();

    $t='Database handle attribute "ReadOnly" allows INSERT queries when switched off';
    $dbh4->{ReadOnly} = 0;
    eval { $dbh4->do($SQL); };
    is ($@, q{}, $t);
    $dbh4->rollback();

    $t='Database handle attribute "ReadOnly" allows INSERT queries when switched off';
    $dbh4->{ReadOnly} = 0;
    eval { $dbh4->do($SQL); };
    is ($@, q{}, $t);
    $dbh4->rollback();

    $dbh4->{ReadOnly} = 1;
    $dbh4->{AutoCommit} = 1;
    $t='Database handle attribute "ReadOnly" has no effect if AutoCommit is on';
    eval { $dbh4->do($SQL); };
    is ($@, q{}, $t);

    my $delete = 'DELETE FROM dbd_pg_test WHERE id = 50';
    $dbh4->do($delete);
    $sth = $dbh4->prepare($SQL);
    eval { $sth->execute(); };
    is ($@, q{}, $t);

    $dbh4->disconnect();
}

#
# Test of the database handle attribute InactiveDestroy
# This one must be the last test performed!
#

$t='Database handle attribute "InactiveDestroy" is set properly';
$attrib = $dbh->{InactiveDestroy};
ok (!$attrib, $t);

# Disconnect in preparation for the fork tests
ok ($dbh->disconnect(), 'Disconnect from database');

$t='Database handle attribute "Active" is false after disconnect';
$attrib = $dbh->{Active};
is ($attrib, '', $t);

SKIP: {
    skip ('Cannot test database handle "AutoInactiveDestroy" on a non-forking system', 8)
        if $^O =~ /Win/;

    require Test::Simple;

    skip ('Test::Simple version 0.47 or better required for testing of attribute "AutoInactiveDestroy"', 8)
        if $Test::Simple::VERSION < 0.47;

    # Test of forking. Hang on to your hats

    my $answer = 42;
    $SQL = "SELECT $answer FROM dbd_pg_test WHERE id > ? LIMIT 1";

    for my $destroy (0,1) {

        $dbh = connect_database({nosetup => 1, AutoCommit => 1 });
        $dbh->{'AutoInactiveDestroy'} = $destroy;
        $dbh->{'pg_server_prepare'} = 1;
        $sth = $dbh->prepare($SQL);
        $sth->execute(1);
        $sth->finish();

        # Desired flow: parent test, child test, child kill, parent test

        if (fork) {
            $t=qq{Parent in fork test is working properly ("AutoInactiveDestroy" = $destroy)};
            $sth->execute(1);
            $val = $sth->fetchall_arrayref()->[0][0];
            is ($val, $answer, $t);
            # Let the child exit first
            select(undef,undef,undef,0.3);
        }
        else { # Child
            select(undef,undef,undef,0.1); # Age before beauty
            exit; ## Calls disconnect via DESTROY unless AutoInactiveDestroy set
        }

        if ($destroy) {
            $t=qq{Ping works after the child has exited ("AutoInactiveDestroy" = $destroy)};
            ok ($dbh->ping(), $t);

            $t='Successful ping returns a SQLSTATE code of 00000 (empty string)';
            my $state = $dbh->state();
            is ($state, '', $t);

            $t='Statement handle works after forking';
            $sth->execute(1);
            $val = $sth->fetchall_arrayref()->[0][0];
            is ($val, $answer, $t);
        }
        else {
            $t=qq{Ping fails after the child has exited ("AutoInactiveDestroy" = $destroy)};
            is ( $dbh->ping(), 0, $t);

            $t=qq{pg_ping gives an error code of -2 after the child has exited ("AutoInactiveDestroy" = $destroy)};
            is ( $dbh->pg_ping(), -2, $t);
            ok ($dbh->disconnect(), 'Disconnect from database');
        }
    }
}

# Disconnect in preparation for the fork tests
ok ($dbh->disconnect(), 'Disconnect from database');

$t='Database handle attribute "Active" is false after disconnect';
$attrib = $dbh->{Active};
is ($attrib, '', $t);

SKIP: {
    skip ('Cannot test database handle "InactiveDestroy" on a non-forking system', 7)
        if $^O =~ /Win/;

    require Test::Simple;

    skip ('Test::Simple version 0.47 or better required for testing of attribute "InactiveDestroy"', 7)
        if $Test::Simple::VERSION < 0.47;

    # Test of forking. Hang on to your hats

    my $answer = 42;
    $SQL = "SELECT $answer FROM dbd_pg_test WHERE id > ? LIMIT 1";

    for my $destroy (0,1) {

        local $SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DESTROY failed: no connection/ }; # shut up destroy warning
        $dbh = connect_database({nosetup => 1, AutoCommit => 1});
        $sth = $dbh->prepare($SQL);
        $sth->execute(1);
        $sth->finish();

        # Desired flow: parent test, child test, child kill, parent test

        if (fork) {
            $t=qq{Parent in fork test is working properly ("InactiveDestroy" = $destroy)};
            $sth->execute(1);
            $val = $sth->fetchall_arrayref()->[0][0];
            is ($val, $answer, $t);
            # Let the child exit first
            select(undef,undef,undef,0.5);
        }
        else { # Child
            $dbh->{InactiveDestroy} = $destroy;
            select(undef,undef,undef,0.1); # Age before beauty
            exit; ## Calls disconnect via DESTROY unless InactiveDestroy set
        }

        if ($destroy) {
            $t=qq{Ping works after the child has exited ("InactiveDestroy" = $destroy)};
            ok ($dbh->ping(), $t);

            $t='Successful ping returns a SQLSTATE code of 00000 (empty string)';
            my $state = $dbh->state();
            is ($state, '', $t);

            $t='Statement handle works after forking';
            $sth->execute(1);
            $val = $sth->fetchall_arrayref()->[0][0];
            is ($val, $answer, $t);
        }
        else {
            $t=qq{Ping fails after the child has exited ("InactiveDestroy" = $destroy)};
            is ( $dbh->ping(), 0, $t);

            $t=qq{pg_ping gives an error code of -2 after the child has exited ("InactiveDestroy" = $destroy)};
            is ( $dbh->pg_ping(), -2,$t);
        }
    }
}

cleanup_database($dbh,'test');
$dbh->disconnect();
