#!perl

## Test of the database handle methods
## The following methods are *not* (explicitly) tested here:
## "take_imp_data"  "pg_server_trace"  "pg_server_untrace"  "pg_type_info"
## "data_sources" (see 04misc.t)
## "disconnect" (see 01connect.t)
## "pg_savepoint"  "pg_release"  "pg_rollback_to" (see 20savepoints.t)
## "pg_getline"  "pg_endcopy"  "pg_getcopydata"  "pg_getcopydata_async" (see 07copy.t)
## "pg_putline"  "pg_putcopydata"  "pg_putcopydata_async (see 07copy.t)
## "pg_cancel"  "pg_ready"  "pg_result" (see 08async.t)

use 5.006;
use strict;
use warnings;
use Data::Dumper;
use Test::More;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 524;

isnt ($dbh, undef, 'Connect to database for database handle method testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
my ($schema,$schema2) = ('dbd_pg_testschema', 'dbd_pg_testschema2');
my ($table1,$table2,$table3) = ('dbd_pg_test1','dbd_pg_test2','dbd_pg_test3');
my ($sequence2,$sequence3,$sequence4) = ('dbd_pg_testsequence2','dbd_pg_testsequence3','dbd_pg_testsequence4');

my ($SQL, $sth, $result, @result, $expected, $warning, $rows, $t, $info);

# Quick simple "tests"


$dbh->do(q{}); ## This used to break, so we keep it as a test...
$SQL = q{SELECT '2529DF6AB8F79407E94445B4BC9B906714964AC8' FROM dbd_pg_test WHERE id=?};
$sth = $dbh->prepare($SQL);
$sth->finish();
$sth = $dbh->prepare_cached($SQL);
$sth->finish();

# Populate the testing table for later use

$SQL = 'INSERT INTO dbd_pg_test(id,val) VALUES (?,?)';

$sth = $dbh->prepare($SQL);
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->execute(10,'Roseapple');
$sth->execute(11,'Pineapple');
$sth->execute(12,'Kiwi');

#
# Test of the "last_insert_id" database handle method
#

$t='DB handle method "last_insert_id" fails when no arguments are given';
$dbh->commit();
eval {
	$dbh->last_insert_id(undef,undef,undef,undef);
};
like ($@, qr{last_insert_id.*least}, $t);

$t='DB handle method "last_insert_id" fails when given a non-existent sequence';
eval {
	$dbh->last_insert_id(undef,undef,undef,undef,{sequence=>'dbd_pg_nonexistentsequence_test'});
};
is ($dbh->state, '42P01', $t);

$t='DB handle method "last_insert_id" fails when given a non-existent table';
$dbh->rollback();
eval {
	$dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef);
};
like ($@, qr{not find}, $t);

$t='DB handle method "last_insert_id" fails when given an arrayref as last argument';
$dbh->rollback();
eval {
	$dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef,[]);
};
like ($@, qr{last_insert_id.*hashref}, $t);

$t='DB handle method "last_insert_id" works when given an empty sequence argument';
$dbh->rollback();
eval {
	$dbh->last_insert_id(undef,undef,'dbd_pg_test',undef,{sequence=>''});
};
is ($@, q{}, $t);

$t='DB handle method "last_insert_id" fails when given a table with no primary key';
$dbh->rollback();
$dbh->do('CREATE TEMP TABLE dbd_pg_test_temp(a int)');
eval {
	$dbh->last_insert_id(undef,undef,'dbd_pg_test_temp',undef);
};
like ($@, qr{last_insert_id}, $t);

$t='DB handle method "last_insert_id" works when given a valid sequence and an invalid table';
$dbh->rollback();
eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef,{sequence=>'dbd_pg_testsequence'});
};
is ($@, q{}, $t);

$t='DB handle method "last_insert_id" returns a numeric value';
like ($result, qr{^\d+$}, $t);

$t='DB handle method "last_insert_id" works when given a valid sequence and an invalid table';
eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef, 'dbd_pg_testsequence');
};
is ($@, q{}, $t);

$t='DB handle method "last_insert_id" returns a numeric value';
like ($result, qr{^\d+$}, $t);

$t='DB handle method "last_insert_id" works when given a valid table';
eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef);
};
is ($@, q{}, $t);

$t='DB handle method "last_insert_id" works when given an empty attrib';
eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef,'');
};
is ($@, q{}, $t);

$t='DB handle method "last_insert_id" works when called twice (cached) given a valid table';
eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef);
};
is ($@, q{}, $t);

$dbh->do("CREATE SCHEMA $schema2");
$dbh->do("CREATE SEQUENCE $schema2.$sequence2");
$dbh->do("CREATE SEQUENCE $schema.$sequence4");
$dbh->{Warn} = 0;
$dbh->do("CREATE TABLE $schema2.$table2(a INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('$schema2.$sequence2'))");
$dbh->do("CREATE TABLE $schema.$table2(a INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('$schema.$sequence4'))");
$dbh->{Warn} = 1;
$dbh->do("INSERT INTO $schema2.$table2 DEFAULT VALUES");

$t='DB handle method "last_insert_id" works when called with a schema not in the search path';
eval {
	$result = $dbh->last_insert_id(undef,$schema2,$table2,undef);
};
is ($@, q{}, $t);

$t='search_path respected when using last_insert_id with no cache (first table)';
$dbh->commit();
$dbh->do("SELECT setval('$schema2.$sequence2',200)");
$dbh->do("SELECT setval('$schema.$sequence4',100)");
$dbh->do("SET search_path = $schema,$schema2");
eval {
	$result = $dbh->last_insert_id(undef,undef,$table2,undef,{pg_cache=>0});
};
is ($@, q{}, $t);
is ($result, 100, $t);

$t='search_path respected when using last_insert_id with no cache (second table)';
$dbh->commit();
$dbh->do("SET search_path = $schema2,$schema");
eval {
	$result = $dbh->last_insert_id(undef,undef,$table2,undef,{pg_cache=>0});
};
is ($@, q{}, $t);
is ($result, 200, $t);

$t='Setting cache on (explicit) returns last result, even if search_path changes';
$dbh->do("SET search_path = $schema,$schema2");
eval {
	$result = $dbh->last_insert_id(undef,undef,$table2,undef,{pg_cache=>1});
};
is ($@, q{}, $t);
is ($result, 200, $t);

$t='Setting cache on (implicit) returns last result, even if search_path changes';
$dbh->do("SET search_path = $schema,$schema2");
eval {
	$result = $dbh->last_insert_id(undef,undef,$table2,undef);
};
is ($@, q{}, $t);
is ($result, 200, $t);

$dbh->commit();
SKIP: {
	$t='DB handle method "last_insert_id" fails when the sequence name is changed and cache is used';

	if ($pgversion < 80300) {
		$dbh->do("DROP TABLE $schema2.$table2");
		$dbh->do("DROP SEQUENCE $schema2.$sequence2");
		skip ('Cannot test sequence rename on pre-8.3 servers', 2);
	}
	$dbh->do("ALTER SEQUENCE $schema2.$sequence2 RENAME TO $sequence3");
	$dbh->commit();
	eval {
		$dbh->last_insert_id(undef,$schema2,$table2,undef);
	};
	like ($@, qr{last_insert_id}, $t);
	$dbh->rollback();

	$t='DB handle method "last_insert_id" works when the sequence name is changed and cache is turned off';
	$dbh->commit();
	eval {
		$dbh->last_insert_id(undef,$schema2,$table2,undef, {pg_cache=>0});
	};
	is ($@, q{}, $t);
	$dbh->do("DROP TABLE $schema2.$table2");
	$dbh->do("DROP SEQUENCE $schema2.$sequence3");
}


$dbh->do("DROP SCHEMA $schema2");
$dbh->do("DROP TABLE $table2");
$dbh->do("DROP SEQUENCE $sequence4");

#
# Test of the "selectrow_array" database handle method
#

$t='DB handle method "selectrow_array" works';
$SQL = 'SELECT id FROM dbd_pg_test ORDER BY id';
@result = $dbh->selectrow_array($SQL);
$expected = [10];
is_deeply (\@result, $expected, $t);

#
# Test of the "selectrow_arrayref" database handle method
#

$t='DB handle method "selectrow_arrayref" works';
$result = $dbh->selectrow_arrayref($SQL);
is_deeply ($result, $expected, $t);

$t='DB handle method "selectrow_arrayref" works with a prepared statement handle';
$sth = $dbh->prepare($SQL);
$result = $dbh->selectrow_arrayref($sth);
is_deeply ($result, $expected, $t);

#
# Test of the "selectrow_hashref" database handle method
#

$t='DB handle method "selectrow_hashref" works';
$result = $dbh->selectrow_hashref($SQL);
$expected = {id => 10};
is_deeply ($result, $expected, $t);

$t='DB handle method "selectrow_hashref" works with a prepared statement handle';
$sth = $dbh->prepare($SQL);
$result = $dbh->selectrow_hashref($sth);
is_deeply ($result, $expected, $t);

#
# Test of the "selectall_arrayref" database handle method
#

$t='DB handle method "selectall_arrayref" works';
$result = $dbh->selectall_arrayref($SQL);
$expected = [[10],[11],[12]];
is_deeply ($result, $expected, $t);

$t='DB handle method "selectall_arrayref" works with a prepared statement handle';
$sth = $dbh->prepare($SQL);
$result = $dbh->selectall_arrayref($sth);
is_deeply ($result, $expected, $t);

$t='DB handle method "selectall_arrayref" works with the MaxRows attribute';
$result = $dbh->selectall_arrayref($SQL, {MaxRows => 2});
$expected = [[10],[11]];
is_deeply ($result, $expected, $t);

$t='DB handle method "selectall_arrayref" works with the Slice attribute';
$SQL = 'SELECT id, val FROM dbd_pg_test ORDER BY id';
$result = $dbh->selectall_arrayref($SQL, {Slice => [1]});
$expected = [['Roseapple'],['Pineapple'],['Kiwi']];
is_deeply ($result, $expected, $t);

#
# Test of the "selectall_hashref" database handle method
#

$t='DB handle method "selectall_hashref" works';
$result = $dbh->selectall_hashref($SQL,'id');
$expected = {10=>{id =>10,val=>'Roseapple'},11=>{id=>11,val=>'Pineapple'},12=>{id=>12,val=>'Kiwi'}};
is_deeply ($result, $expected, $t);

$t='DB handle method "selectall_hashref" works with a prepared statement handle';
$sth = $dbh->prepare($SQL);
$result = $dbh->selectall_hashref($sth,'id');
is_deeply ($result, $expected, $t);

#
# Test of the "selectcol_arrayref" database handle method
#

$t='DB handle method "selectcol_arrayref" works';
$result = $dbh->selectcol_arrayref($SQL);
$expected = [10,11,12];
is_deeply ($result, $expected, $t);

$t='DB handle method "selectcol_arrayref" works with a prepared statement handle';
$result = $dbh->selectcol_arrayref($sth);
is_deeply ($result, $expected, $t);

$t='DB handle method "selectcol_arrayref" works with the Columns attribute';
$result = $dbh->selectcol_arrayref($SQL, {Columns=>[2,1]});
$expected = ['Roseapple',10,'Pineapple',11,'Kiwi',12];
is_deeply ($result, $expected, $t);

$t='DB handle method "selectcol_arrayref" works with the MaxRows attribute';
$result = $dbh->selectcol_arrayref($SQL, {Columns=>[2], MaxRows => 1});
$expected = ['Roseapple'];
is_deeply ($result, $expected, $t);

#
# Test of the "commit" and "rollback" database handle methods
#

{
	local $SIG{__WARN__} = sub { $warning = shift; };
	$dbh->{AutoCommit}=0;

	$t='DB handle method "commit" gives no warning when AutoCommit is off';
	$warning=q{};
	$dbh->commit();
	ok (! length $warning, $t);

	$t='DB handle method "rollback" gives no warning when AutoCommit is off';
	$warning=q{};
	$dbh->rollback();
	ok (! length $warning, $t);

	$t='DB handle method "commit" returns true';
	ok ($dbh->commit, $t);

	$t='DB handle method "rollback" returns true';
	ok ($dbh->rollback, $t);

	$t='DB handle method "commit" gives a warning when AutoCommit is on';
	$dbh->{AutoCommit}=1;
	$warning=q{};
	$dbh->commit();
	ok (length $warning, $t);

	$t='DB handle method "rollback" gives a warning when AutoCommit is on';
	$warning=q{};
	$dbh->rollback();
	ok (length $warning, $t);
}

#
# Test of the "begin_work" database handle method
#

$t='DB handle method "begin_work" gives a warning when AutoCommit is on';
$dbh->{AutoCommit}=0;
eval {
	$dbh->begin_work();
};
isnt ($@, q{}, $t);

$t='DB handle method "begin_work" gives no warning when AutoCommit is off';
$dbh->{AutoCommit}=1;
eval {
	$dbh->begin_work();
};
is ($@, q{}, $t);
ok (!$dbh->{AutoCommit}, 'DB handle method "begin_work" sets AutoCommit to off');

$t='DB handle method "commit" after "begin_work" sets AutoCommit to on';
$dbh->commit();
ok ($dbh->{AutoCommit}, $t);

$t='DB handle method "begin_work" gives no warning when AutoCommit is off';
$dbh->{AutoCommit}=1;
eval {
	$dbh->begin_work();
};
is ($@, q{}, $t);

$t='DB handle method "begin_work" sets AutoCommit to off';
ok (!$dbh->{AutoCommit}, $t);

$t='DB handle method "rollback" after "begin_work" sets AutoCommit to on';
$dbh->rollback();
ok ($dbh->{AutoCommit}, $t);

$dbh->{AutoCommit}=0;

#
# Test of the "get_info" database handle method
#

$t='DB handle method "get_info" with no arguments gives an error';
eval {
  $dbh->get_info();
};
isnt ($@, q{}, $t);

my %get_info = (
  SQL_MAX_DRIVER_CONNECTIONS =>  0,
  SQL_DRIVER_NAME            =>  6,
  SQL_DBMS_NAME              => 17,
  SQL_DBMS_VERSION           => 18,
  SQL_IDENTIFIER_QUOTE_CHAR  => 29,
  SQL_CATALOG_NAME_SEPARATOR => 41,
  SQL_USER_NAME              => 47,
);

for (keys %get_info) {
	$t=qq{DB handle method "get_info" works with a value of "$_"};
	my $back = $dbh->get_info($_);
	ok (defined $back, $t);

	$t=qq{DB handle method "get_info" works with a value of "$get_info{$_}"};
	my $forth = $dbh->get_info($get_info{$_});
	ok (defined $forth, $t);

	$t=q{DB handle method "get_info" returned matching values};
	is ($back, $forth, $t);
}

# Make sure odbcversion looks normal
$t='DB handle method "get_info" returns a valid looking ODBCVERSION string}';
my $odbcversion = $dbh->get_info(18);
like ($odbcversion, qr{^([1-9]\d|\d[1-9])\.\d\d\.\d\d00$}, $t);

# Testing max connections is good as this info is dynamic
$t='DB handle method "get_info" returns a number for SQL_MAX_DRIVER_CONNECTIONS';
my $maxcon = $dbh->get_info(0);
like ($maxcon, qr{^\d+$}, $t);

$t='DB handle method "get_info" returns correct string for SQL_DATA_SOURCE_READ_ONLY when "on"';
$dbh->do(q{SET transaction_read_only = 'on'});
is ($dbh->get_info(25), 'Y', $t);

$t='DB handle method "get_info" returns correct string for SQL_DATA_SOURCE_READ_ONLY when "off"';
$dbh->do(q{SET transaction_read_only = 'off'});
is ($dbh->get_info(25), 'N', $t);

#
# Test of the "table_info" database handle method
#

$t='DB handle method "table_info" works when called with undef arguments';
$sth = $dbh->table_info('', '', 'dbd_pg_test', '');
my $number = $sth->rows();
ok ($number, $t);

# Check required minimum fields
$t='DB handle method "table_info" returns fields required by DBI';
$result = $sth->fetchall_arrayref({});
my @required = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME TABLE_TYPE REMARKS));
my %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply (\%missing, {}, $t);

## Check some of the returned fields:
$result = $result->[0];
is ($result->{TABLE_CAT}, undef, 'DB handle method "table_info" returns proper TABLE_CAT');
is ($result->{TABLE_NAME}, 'dbd_pg_test', 'DB handle method "table_info" returns proper TABLE_NAME');
is ($result->{TABLE_TYPE}, 'TABLE', 'DB handle method "table_info" returns proper TABLE_TYPE');

$t=q{DB handle method "table_info" returns correct number of rows when given a 'TABLE,VIEW' type argument};
$sth = $dbh->table_info(undef,undef,undef,'TABLE,VIEW');
$number = $sth->rows();
cmp_ok ($number, '>', 1, $t);

$t='DB handle method "table_info" returns correct number of rows when given an invalid type argument';
$sth = $dbh->table_info(undef,undef,undef,'DUMMY');
$rows = $sth->rows();
is ($rows, $number, $t);

$t=q{DB handle method "table_info" returns correct number of rows when given a 'VIEW' type argument};
$sth = $dbh->table_info(undef,undef,undef,'VIEW');
$rows = $sth->rows();
cmp_ok ($rows, '<', $number, $t);

$t=q{DB handle method "table_info" returns correct number of rows when given a 'TABLE' type argument};
$sth = $dbh->table_info(undef,undef,undef,'TABLE');
$rows = $sth->rows();
cmp_ok ($rows, '<', $number, $t);

# Test listing catalog names
$t='DB handle method "table_info" works when called with a catalog of %';
$sth = $dbh->table_info('%', '', '');
ok ($sth, $t);

# Test listing schema names
$t='DB handle method "table_info" works when called with a schema of %';
$sth = $dbh->table_info('', '%', '');
ok ($sth, $t);

# Test listing table types
$t='DB handle method "table_info" works when called with a type of %';
$sth = $dbh->table_info('', '', '', '%');
ok ($sth, $t);

#
# Test of the "column_info" database handle method
#

# Check required minimum fields
$t='DB handle method "column_info" returns fields required by DBI';
$sth = $dbh->column_info('','','dbd_pg_test','score');
$result = $sth->fetchall_arrayref({});
@required =
	(qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME DATA_TYPE 
            TYPE_NAME COLUMN_SIZE BUFFER_LENGTH DECIMAL_DIGITS 
            NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF SQL_DATA_TYPE
         SQL_DATETIME_SUB CHAR_OCTET_LENGTH ORDINAL_POSITION
         IS_NULLABLE));
undef %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply (\%missing, {}, $t);

# Check that pg_constraint was populated
$t=q{DB handle method "column info" 'pg_constraint' returns a value for constrained columns};
$result = $result->[0];
like ($result->{pg_constraint}, qr/score/, $t);

# Check that it is not populated for non-constrained columns
$t=q{DB handle method "column info" 'pg_constraint' returns undef for non-constrained columns};
$sth = $dbh->column_info('','','dbd_pg_test','id');
$result = $sth->fetchall_arrayref({})->[0];
is ($result->{pg_constraint}, undef, $t);

# Check the rest of the custom "pg" columns
$t=q{DB handle method "column_info" returns good value for 'pg_type'};
is ($result->{pg_type}, 'integer', $t);

## Check some of the returned fields:
my $r = $result;
is ($r->{TABLE_CAT},   undef,               'DB handle method "column_info" returns proper TABLE_CAT');
is ($r->{TABLE_NAME},  'dbd_pg_test',       'DB handle method "column_info returns proper TABLE_NAME');
is ($r->{COLUMN_NAME}, 'id',                'DB handle method "column_info" returns proper COLUMN_NAME');
is ($r->{DATA_TYPE},   4,                   'DB handle method "column_info" returns proper DATA_TYPE');
is ($r->{COLUMN_SIZE}, 4,                   'DB handle method "column_info" returns proper COLUMN_SIZE');
is ($r->{NULLABLE},    '0',                 'DB handle method "column_info" returns proper NULLABLE');
is ($r->{REMARKS},     'Bob is your uncle', 'DB handle method "column_info" returns proper REMARKS');
is ($r->{COLUMN_DEF},  undef,               'DB handle method "column_info" returns proper COLUMN_DEF');
is ($r->{IS_NULLABLE}, 'NO',                'DB handle method "column_info" returns proper IS_NULLABLE');
is ($r->{pg_type},     'integer',           'DB handle method "column_info" returns proper pg_type');
is ($r->{ORDINAL_POSITION}, 1,              'DB handle method "column_info" returns proper ORDINAL_POSITION');

# Make sure we handle CamelCase Column Correctly
$t=q{DB handle method "column_info" works with non-lowercased columns};
$sth = $dbh->column_info('','','dbd_pg_test','CaseTest');
$result = $sth->fetchall_arrayref({})->[0];
is ($result->{COLUMN_NAME}, q{"CaseTest"}, $t);

SKIP: {

	if ($pgversion < 80300) {
		skip ('DB handle method column_info attribute "pg_enum_values" requires at least Postgres 8.3', 2);
	}

    {
        local $dbh->{Warn} = 0;

        $dbh->do( q{CREATE TYPE dbd_pg_enumerated AS ENUM ('foo', 'bar', 'baz', 'buz')} );
        $dbh->do( q{CREATE TEMP TABLE dbd_pg_enum_test ( is_enum dbd_pg_enumerated NOT NULL )} );
    }

	$t='DB handle method "column_info" returns proper pg_type';
    $sth = $dbh->column_info('','','dbd_pg_enum_test','is_enum');
    $result = $sth->fetchall_arrayref({})->[0];
    is ($result->{pg_type}, 'dbd_pg_enumerated', $t);

	$t='DB handle method "column_info" returns proper pg_enum_values';
    is_deeply ($result->{pg_enum_values}, [ qw( foo bar baz buz ) ], $t);

	$dbh->do('DROP TABLE dbd_pg_enum_test');
	$dbh->do('DROP TYPE dbd_pg_enumerated');
}

#
# Test of the "primary_key_info" database handle method
#

# Check required minimum fields
$t='DB handle method "primary_key_info" returns required fields';
$sth = $dbh->primary_key_info('','','dbd_pg_test');
$result = $sth->fetchall_arrayref({});
@required = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME KEY_SEQ PK_NAME DATA_TYPE));
undef %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply (\%missing, {}, $t);

## Check some of the returned fields:
$result = $result->[0];
is ($r->{TABLE_CAT},   undef,         'DB handle method "primary_key_info" returns proper TABLE_CAT');
is ($r->{TABLE_NAME},  'dbd_pg_test', 'DB handle method "primary_key_info" returns proper TABLE_NAME');
is ($r->{COLUMN_NAME}, 'id',          'DB handle method "primary_key_info" returns proper COLUMN_NAME');
cmp_ok ($result->{KEY_SEQ}, '>=', 1,  'DB handle method "primary_key_info" returns proper KEY_SEQ');

#
# Test of the "primary_key" database handle method
#

$t='DB handle method "primary_key" works';
@result = $dbh->primary_key('', '', 'dbd_pg_test');
$expected = ['id'];
is_deeply (\@result, $expected, $t);

$t='DB handle method "primary_key" returns empty list for invalid table';
@result = $dbh->primary_key('', '', 'dbd_pg_test_do_not_create_this_table');
$expected = [];
is_deeply (\@result, $expected, $t);

#
# Test of the "statistics_info" database handle method
#

SKIP: {

$dbh->{private_dbdpg}{version} >= 80000
	or skip ('Server must be version 8.0 or higher to test database handle method "statistics_info"', 10);

$t='DB handle method "statistics_info" returns undef: no table';
$sth = $dbh->statistics_info(undef,undef,undef,undef,undef);
is ($sth, undef, $t);

## Invalid table
$t='DB handle method "statistics_info" returns undef: bad table';
$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test9',undef,undef);
is ($sth, undef, $t);

## Create some tables with various indexes
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("CREATE TABLE $table1 (a INT, b INT NOT NULL, c INT NOT NULL, ".
			 'CONSTRAINT dbd_pg_test1_pk PRIMARY KEY (a))');
	$dbh->do("ALTER TABLE $table1 ADD CONSTRAINT dbd_pg_test1_uc1 UNIQUE (b)");
	$dbh->do("CREATE UNIQUE INDEX dbd_pg_test1_index_c ON $table1(c)");
	$dbh->do("CREATE TABLE $table2 (a INT, b INT, c INT, PRIMARY KEY(a,b), UNIQUE(b,c))");
	$dbh->do("CREATE INDEX dbd_pg_test2_skipme ON $table2(c,(a+b))");
	$dbh->do("CREATE TABLE $table3 (a INT, b INT, c INT, PRIMARY KEY(a)) WITH OIDS");
	$dbh->do("CREATE UNIQUE INDEX dbd_pg_test3_index_b ON $table3(b)");
	$dbh->do("CREATE INDEX dbd_pg_test3_index_c ON $table3 USING hash(c)");
	$dbh->do("CREATE INDEX dbd_pg_test3_oid ON $table3(oid)");
	$dbh->do("CREATE UNIQUE INDEX dbd_pg_test3_pred ON $table3(c) WHERE c > 0 AND c < 45");
	$dbh->commit();
}

my $correct_stats = {
one => [
	[ undef, $schema, $table1, undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, $schema, $table1, '0', undef, 'dbd_pg_test1_index_c', 'btree',  1, 'c', 'A', '0', '1', undef ],
	[ undef, $schema, $table1, '0', undef, 'dbd_pg_test1_pk',      'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, $schema, $table1, '0', undef, 'dbd_pg_test1_uc1',     'btree',  1, 'b', 'A', '0', '1', undef ],
	],
	two => [
	[ undef, $schema, $table2, undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, $schema, $table2, '0', undef, 'dbd_pg_test2_b_key',   'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, $schema, $table2, '0', undef, 'dbd_pg_test2_b_key',   'btree',  2, 'c', 'A', '0', '1', undef ],
	[ undef, $schema, $table2, '0', undef, 'dbd_pg_test2_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, $schema, $table2, '0', undef, 'dbd_pg_test2_pkey',    'btree',  2, 'b', 'A', '0', '1', undef ],
	],
	three => [
	[ undef, $schema, $table3, undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_index_b', 'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_pred',    'btree',  1, 'c', 'A', '0', '1', '((c > 0) AND (c < 45))' ],
	[ undef, $schema, $table3, '1', undef, 'dbd_pg_test3_oid',     'btree',  1, 'oid', 'A', '0', '1', undef ],
	[ undef, $schema, $table3, '1', undef, 'dbd_pg_test3_index_c', 'hashed', 1, 'c', 'A', '0', '4', undef ],
],
	three_uo => [
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_index_b', 'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, $schema, $table3, '0', undef, 'dbd_pg_test3_pred',    'btree',  1, 'c', 'A', '0', '1', '((c > 0) AND (c < 45))' ],
	],
};

my $stats;

$t="Correct stats output for $table1";
$sth = $dbh->statistics_info(undef,$schema,$table1,undef,undef);
$stats = $sth->fetchall_arrayref;
is_deeply ($stats, $correct_stats->{one}, $t);

$t="Correct stats output for $table2";
$sth = $dbh->statistics_info(undef,$schema,$table2,undef,undef);
$stats = $sth->fetchall_arrayref;
is_deeply ($stats, $correct_stats->{two}, $t);

$t="Correct stats output for $table3";
$sth = $dbh->statistics_info(undef,$schema,$table3,undef,undef);
$stats = $sth->fetchall_arrayref;
## Too many intra-version differences to try for an exact number here:
$correct_stats->{three}[5][11] = $stats->[5][11] = 0;
is_deeply ($stats, $correct_stats->{three}, $t);

$t="Correct stats output for $table3 (unique only)";
$sth = $dbh->statistics_info(undef,$schema,$table3,1,undef);
$stats = $sth->fetchall_arrayref;
is_deeply ($stats, $correct_stats->{three_uo}, $t);

{
	$t="Correct stats output for $table1";
	$sth = $dbh->statistics_info(undef,undef,$table1,undef,undef);
	$stats = $sth->fetchall_arrayref;
	is_deeply ($stats, $correct_stats->{one}, $t);

	$t="Correct stats output for $table3";
	$sth = $dbh->statistics_info(undef,undef,$table2,undef,undef);
	$stats = $sth->fetchall_arrayref;
	is_deeply ($stats, $correct_stats->{two}, $t);

	$t="Correct stats output for $table3";
	$sth = $dbh->statistics_info(undef,undef,$table3,undef,undef);
	$stats = $sth->fetchall_arrayref;
	$correct_stats->{three}[5][11] = $stats->[5][11] = 0;
	is_deeply ($stats, $correct_stats->{three}, $t);

	$t="Correct stats output for $table3 (unique only)";
	$sth = $dbh->statistics_info(undef,undef,$table3,1,undef);
	$stats = $sth->fetchall_arrayref;
	is_deeply ($stats, $correct_stats->{three_uo}, $t);
}

# Clean everything up
$dbh->do("DROP TABLE $table3");
$dbh->do("DROP TABLE $table2");
$dbh->do("DROP TABLE $table1");

} ## end of statistics_info tests


#
# Test of the "foreign_key_info" database handle method
#

## Neither pktable nor fktable specified
$t='DB handle method "foreign_key_info" returns undef: no pk / no fk';
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,undef);
is ($sth, undef, $t);

# Drop any tables that may exist
my $fktables = join ',' => map { "'dbd_pg_test$_'" } (1..3);
$SQL = "SELECT relname FROM pg_catalog.pg_class WHERE relkind='r' AND relname IN ($fktables)";
{
	local $SIG{__WARN__} = sub {};
	for (@{$dbh->selectall_arrayref($SQL)}) {
		$dbh->do("DROP TABLE $_->[0] CASCADE");
	}
}
## Invalid primary table
$t='DB handle method "foreign_key_info" returns undef: bad pk / no fk';
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,undef);
is ($sth, undef, $t);

## Invalid foreign table
$t='DB handle method "foreign_key_info" returns undef: no pk / bad fk';
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,'dbd_pg_test9');
is ($sth, undef, $t);

## Both primary and foreign are invalid
$t='DB handle method "foreign_key_info" returns undef: bad fk / bad fk';
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,'dbd_pg_test9');
is ($sth, undef, $t);

## Create a pk table
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('CREATE TABLE dbd_pg_test1 (a INT, b INT NOT NULL, c INT NOT NULL, '.
			 'CONSTRAINT dbd_pg_test1_pk PRIMARY KEY (a))');
	$dbh->do('ALTER TABLE dbd_pg_test1 ADD CONSTRAINT dbd_pg_test1_uc1 UNIQUE (b)');
	$dbh->do('CREATE UNIQUE INDEX dbd_pg_test1_index_c ON dbd_pg_test1(c)');
	$dbh->commit();
}

## Good primary with no foreign keys
$t='DB handle method "foreign_key_info" returns undef: good pk (but unreferenced)';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
is ($sth, undef, $t);

## Create a simple foreign key table
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('CREATE TABLE dbd_pg_test2 (f1 INT PRIMARY KEY, f2 INT NOT NULL, f3 INT NOT NULL)');
	$dbh->do('ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk1 FOREIGN KEY(f2) REFERENCES dbd_pg_test1(a)');
	$dbh->commit();
}

## Bad primary with good foreign
$t='DB handle method "foreign_key_info" returns undef: bad pk / good fk';
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,$table2);
is ($sth, undef, $t);

## Good primary, good foreign, bad schemas
$t='DB handle method "foreign_key_info" returns undef: good pk / good fk / bad pk schema';
my $testschema = 'dbd_pg_test_badschema11';
$sth = $dbh->foreign_key_info(undef,$testschema,$table1,undef,undef,$table2);
is ($sth, undef, $t);

$t='DB handle method "foreign_key_info" returns undef: good pk / good fk / bad fk schema';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,$testschema,$table2);
is ($sth, undef, $t);

## Good primary
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
$result = $sth->fetchall_arrayref({});

# Check required minimum fields
$t='DB handle method "foreign_key_info" returns fields required by DBI';
$result = $sth->fetchall_arrayref({});
@required =
	(qw(UK_TABLE_CAT UK_TABLE_SCHEM UK_TABLE_NAME PK_COLUMN_NAME 
            FK_TABLE_CAT FK_TABLE_SCHEM FK_TABLE_NAME FK_COLUMN_NAME 
            ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME UK_NAME
            DEFERABILITY UNIQUE_OR_PRIMARY UK_DATA_TYPE FK_DATA_TYPE));
undef %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply (\%missing, {}, $t);

## Good primary
$t='DB handle method "foreign_key_info" works for good pk';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk1 = [
					 undef, ## Catalog
					 $schema, ## Schema
					 $table1, ## Table
					 'a', ## Column
					 undef, ## FK Catalog
					 $schema, ## FK Schema
					 $table2, ## FK Table
					 'f2', ## FK Table
					 2, ## Ordinal position
					 3, ## Update rule
					 3, ## Delete rule
					 'dbd_pg_test2_fk1', ## FK name
					 'dbd_pg_test1_pk',  ## UK name
					 '7', ## deferability
					 'PRIMARY', ## unique or primary
					 'int4', ## uk data type
					 'int4'  ## fk data type
					];
$expected = [$fk1];
is_deeply ($result, $expected, $t);

## Same with explicit table
$t='DB handle method "foreign_key_info" works for good pk / good fk';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table2);
$result = $sth->fetchall_arrayref();
is_deeply ($result, $expected, $t);

## Foreign table only
$t='DB handle method "foreign_key_info" works for good fk';
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,$table2);
$result = $sth->fetchall_arrayref();
is_deeply ($result, $expected, $t);

## Add a foreign key to an explicit unique constraint
$t='DB handle method "foreign_key_info" works for good pk / explicit fk';
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk2 FOREIGN KEY (f3) '.
					 'REFERENCES dbd_pg_test1(b) ON DELETE SET NULL ON UPDATE CASCADE');
}
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk2 = [
					 undef,
					 $schema,
					 $table1,
					 'b',
					 undef,
					 $schema,
					 $table2,
					 'f3',
					 '3',
					 '0', ## cascade
					 '2', ## set null
					 'dbd_pg_test2_fk2',
					 'dbd_pg_test1_uc1',
					 '7',
					 'UNIQUE',
					 'int4',
					 'int4'
          ];
$expected = [$fk1,$fk2];
is_deeply ($result, $expected, $t);

## Add a foreign key to an implicit unique constraint (a unique index on a column)
$t='DB handle method "foreign_key_info" works for good pk / implicit fk';
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_aafk3 FOREIGN KEY (f3) '.
					 'REFERENCES dbd_pg_test1(c) ON DELETE RESTRICT ON UPDATE SET DEFAULT');
}
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk3 = [
					 undef,
					 $schema,
					 $table1,
					 'c',
					 undef,
					 $schema,
					 $table2,
					 'f3',
					 '3',
					 '4', ## set default
					 '1', ## restrict
					 'dbd_pg_test2_aafk3',
					 undef, ## plain indexes have no named constraint
					 '7',
					 'UNIQUE',
					 'int4',
					 'int4'
          ];
$expected = [$fk3,$fk1,$fk2];
is_deeply ($result, $expected, $t);

## Create another foreign key table to point to the first (primary) table
$t='DB handle method "foreign_key_info" works for multiple fks';
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('CREATE TABLE dbd_pg_test3 (ff1 INT NOT NULL)');
	$dbh->do('ALTER TABLE dbd_pg_test3 ADD CONSTRAINT dbd_pg_test3_fk1 FOREIGN KEY(ff1) REFERENCES dbd_pg_test1(a)');
	$dbh->commit();
}

$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk4 = [
					 undef,
					 $schema,
					 $table1,
					 'a',
					 undef,
					 $schema,
					 $table3,
					 'ff1',
					 '1',
					 '3',
					 '3',
					 'dbd_pg_test3_fk1',
					 'dbd_pg_test1_pk',
					 '7',
					 'PRIMARY',
					 'int4',
					 'int4'
          ];
$expected = [$fk3,$fk1,$fk2,$fk4];
is_deeply ($result, $expected, $t);

## Test that explicit naming two tables brings back only those tables
$t='DB handle method "foreign_key_info" works for good pk / good fk (only)';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table3);
$result = $sth->fetchall_arrayref();
$expected = [$fk4];
is_deeply ($result, $expected, $t);

## Multi-column madness
$t='DB handle method "foreign_key_info" works for multi-column keys';
{
	local $SIG{__WARN__} = sub {};
	$dbh->do('ALTER TABLE dbd_pg_test1 ADD CONSTRAINT dbd_pg_test1_uc2 UNIQUE (b,c,a)');
	$dbh->do('ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk4 ' .
					 'FOREIGN KEY (f1,f3,f2) REFERENCES dbd_pg_test1(c,a,b)');
}

$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table2);
$result = $sth->fetchall_arrayref();
## "dbd_pg_test2_fk4" FOREIGN KEY (f1, f3, f2) REFERENCES dbd_pg_test1(c, a, b)
my $fk5 = [
					 undef,
					 $schema,
					 $table1,
					 'c',
					 undef,
					 $schema,
					 $table2,
					 'f1',
					 '1',
					 '3',
					 '3',
					 'dbd_pg_test2_fk4',
					 'dbd_pg_test1_uc2',
					 '7',
					 'UNIQUE',
					 'int4',
					 'int4'
          ];
# For the rest of the multi-column, only change:
# primary column name [3]
# foreign column name [7]
# ordinal position [8]
my @fk6 = @$fk5; my $fk6 = \@fk6; $fk6->[3] = 'a'; $fk6->[7] = 'f3'; $fk6->[8] = 3;
my @fk7 = @$fk5; my $fk7 = \@fk7; $fk7->[3] = 'b'; $fk7->[7] = 'f2'; $fk7->[8] = 2;
$expected = [$fk3,$fk1,$fk2,$fk5,$fk6,$fk7];
is_deeply ($result, $expected, $t);

$t='DB handle method "foreign_key_info" works with FetchHashKeyName NAME_lc';
$dbh->{FetchHashKeyName} = 'NAME_lc';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table2);
$sth->execute();
$result = $sth->fetchrow_hashref();
$sth->finish();
ok (exists $result->{'fk_table_name'}, $t);

$t='DB handle method "foreign_key_info" works with FetchHashKeyName NAME_uc';
$dbh->{FetchHashKeyName} = 'NAME_uc';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table2);
$sth->execute();
$result = $sth->fetchrow_hashref();
ok (exists $result->{'FK_TABLE_NAME'}, $t);

$t='DB handle method "foreign_key_info" works with FetchHashKeyName NAME';
$dbh->{FetchHashKeyName} = 'NAME';
$sth = $dbh->foreign_key_info(undef,undef,$table1,undef,undef,$table2);
$sth->execute();
$result = $sth->fetchrow_hashref();
ok (exists $result->{'FK_TABLE_NAME'}, $t);

# Clean everything up
{
	$dbh->do('DROP TABLE dbd_pg_test3');
	$dbh->do('DROP TABLE dbd_pg_test2');
	$dbh->do('DROP TABLE dbd_pg_test1');
}

#
# Test of the "tables" database handle method
#

$t='DB handle method "tables" works';
@result = $dbh->tables('', '', 'dbd_pg_test', '');
like ($result[0], qr/dbd_pg_test/, $t);

$t='DB handle method "tables" works with a "pg_noprefix" attribute';
@result = $dbh->tables('', '', 'dbd_pg_test', '', {pg_noprefix => 1});
is ($result[0], 'dbd_pg_test', $t);

#
# Test of the "type_info_all" database handle method
#

$result = $dbh->type_info_all();

# Quick check that the structure looks correct
$t='DB handle method "type_info_all" returns a valid structure';
my $badresult=q{};
if (ref $result eq 'ARRAY') {
	my $index = $result->[0];
	if (ref $index ne 'HASH') {
		$badresult = 'First element in array not a hash ref';
	}
	else {
		for (qw(TYPE_NAME DATA_TYPE CASE_SENSITIVE)) {
			$badresult = "Field $_ missing" if !exists $index->{$_};
		}
	}
}
else {
	$badresult = 'Array reference not returned';
}
diag "type_info_all problem: $badresult" if $badresult;
ok (!$badresult, $t);

#
# Test of the "type_info" database handle method
#

# Check required minimum fields
$t='DB handle method "type_info" returns fields required by DBI';
$result = $dbh->type_info(4);
@required =
	(qw(TYPE_NAME DATA_TYPE COLUMN_SIZE LITERAL_PREFIX LITERAL_SUFFIX 
            CREATE_PARAMS NULLABLE CASE_SENSITIVE SEARCHABLE UNSIGNED_ATTRIBUTE 
            FIXED_PREC_SCALE AUTO_UNIQUE_VALUE LOCAL_TYPE_NAME MINIMUM_SCALE 
            MAXIMUM_SCALE SQL_DATA_TYPE SQL_DATETIME_SUB NUM_PREC_RADIX 
            INTERVAL_PRECISION));
undef %missing;
for (@required) {
	$missing{$_}++ if ! exists $result->{$_};
}
is_deeply (\%missing, {}, $t);

#
# Test of the "quote" database handle method
#

my %quotetests = (
	q{0} => q{'0'},
	q{Ain't misbehaving } => q{'Ain''t misbehaving '},
	NULL => q{'NULL'},
	"" => q{''}, ## no critic
);

for (keys %quotetests) {
	$t=qq{DB handle method "quote" works with a value of "$_"};
	$result = $dbh->quote($_);
	is ($result, $quotetests{$_}, $t);
}

## Test timestamp - should quote as a string
$t='DB handle method "quote" work on timestamp';
my $tstype = 93;
my $testtime = '2006-01-28 11:12:13';
is ($dbh->quote( $testtime, $tstype ), qq{'$testtime'}, $t);

$t='DB handle method "quote" works with an undefined value';
my $foo;
{
	no warnings; ## Perl does not like undef args
	is ($dbh->quote($foo), q{NULL}, $t);
}
$t='DB handle method "quote" works with a supplied data type argument';
is ($dbh->quote(1, 4), 1, $t);

## Test bytea quoting
my $scs = $dbh->{pg_standard_conforming_strings};
for my $byteval (1 .. 255) {
	my $byte = chr($byteval);
	$result = $dbh->quote($byte, { pg_type => PG_BYTEA });
	if ($byteval < 32 or $byteval >= 127) {
		$expected = $scs
			? sprintf q{E'\\\\%03o'}, $byteval
				: sprintf q{'\\\\%03o'}, $byteval;
	}
	else {
		$expected = $scs
			? sprintf q{E'%s'}, $byte
				: sprintf q{'%s'}, $byte;
	}
	if ($byte eq '\\') {
		$expected =~ s{\\}{\\\\\\\\};
	}
	elsif ($byte eq q{'}) {
		$expected = $scs ? q{E''''} : q{''''};
	}
	$t = qq{Byte value $byteval quotes to $expected};
	is ($result, $expected, $t);
}

## Various backslash tests
$t='DB handle method "quote" works properly with backslashes';
my $E = $pgversion >= 80100 ? q{E} : q{};
is ($dbh->quote('foo\\bar'), qq{${E}'foo\\\\bar'}, $t);

$t='DB handle method "quote" works properly without backslashes';
is ($dbh->quote('foobar'), q{'foobar'}, $t);

#
# Test various quote types
#


## Points
$t='DB handle method "quote" works with type PG_POINT';
eval { $result = $dbh->quote(q{123,456}, { pg_type => PG_POINT }); };
is ($@, q{}, $t);

$t='DB handle method "quote" returns correct value for type PG_POINT';
is ($result, q{'123,456'}, $t);

$t='DB handle method "quote" fails with invalid PG_POINT string';
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_POINT }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_POINT string';
eval { $result = $dbh->quote(q{A123,456}, { pg_type => PG_POINT }); };
like ($@, qr{Invalid input for geometric type}, $t);

## Lines and line segments
$t='DB handle method "quote" works with valid PG_LINE string';
eval { $result = $dbh->quote(q{123,456}, { pg_type => PG_LINE }); };
is ($@, q{}, $t);

$t='DB handle method "quote" fails with invalid PG_LINE string';
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_LINE }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_LINE string';
eval { $result = $dbh->quote(q{<123,456}, { pg_type => PG_LINE }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_LSEG string';
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_LSEG }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_LSEG string';
eval { $result = $dbh->quote(q{[123,456}, { pg_type => PG_LSEG }); };
like ($@, qr{Invalid input for geometric type}, $t);

## Boxes
$t='DB handle method "quote" works with valid PG_BOX string';
eval { $result = $dbh->quote(q{1,2,3,4}, { pg_type => PG_BOX }); };
is ($@, q{}, $t);

$t='DB handle method "quote" fails with invalid PG_BOX string';
eval { $result = $dbh->quote(q{[1,2,3,4]}, { pg_type => PG_BOX }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_BOX string';
eval { $result = $dbh->quote(q{1,2,3,4,cheese}, { pg_type => PG_BOX }); };
like ($@, qr{Invalid input for geometric type}, $t);

## Paths - can have optional square brackets
$t='DB handle method "quote" works with valid PG_PATH string';
eval { $result = $dbh->quote(q{[(1,2),(3,4)]}, { pg_type => PG_PATH }); };
is ($@, q{}, $t);

$t='DB handle method "quote" returns correct value for type PG_PATH';
is ($result, q{'[(1,2),(3,4)]'}, $t);

$t='DB handle method "quote" fails with invalid PG_PATH string';
eval { $result = $dbh->quote(q{<(1,2),(3,4)>}, { pg_type => PG_PATH }); };
like ($@, qr{Invalid input for path type}, $t);

$t='DB handle method "quote" fails with invalid PG_PATH string';
eval { $result = $dbh->quote(q{<1,2,3,4>}, { pg_type => PG_PATH }); };
like ($@, qr{Invalid input for path type}, $t);

## Polygons
$t='DB handle method "quote" works with valid PG_POLYGON string';
eval { $result = $dbh->quote(q{1,2,3,4}, { pg_type => PG_POLYGON }); };
is ($@, q{}, $t);

$t='DB handle method "quote" fails with invalid PG_POLYGON string';
eval { $result = $dbh->quote(q{[1,2,3,4]}, { pg_type => PG_POLYGON }); };
like ($@, qr{Invalid input for geometric type}, $t);

$t='DB handle method "quote" fails with invalid PG_POLYGON string';
eval { $result = $dbh->quote(q{1,2,3,4,cheese}, { pg_type => PG_POLYGON }); };
like ($@, qr{Invalid input for geometric type}, $t);

## Circles - can have optional angle brackets
$t='DB handle method "quote" works with valid PG_CIRCLE string';
eval { $result = $dbh->quote(q{<(1,2,3)>}, { pg_type => PG_CIRCLE }); };
is ($@, q{}, $t);

$t='DB handle method "quote" returns correct value for type PG_CIRCLE';
is ($result, q{'<(1,2,3)>'}, $t);

$t='DB handle method "quote" fails with invalid PG_CIRCLE string';
eval { $result = $dbh->quote(q{[(1,2,3)]}, { pg_type => PG_CIRCLE }); };
like ($@, qr{Invalid input for circle type}, $t);

$t='DB handle method "quote" fails with invalid PG_CIRCLE string';
eval { $result = $dbh->quote(q{1,2,3,4,H}, { pg_type => PG_CIRCLE }); };
like ($@, qr{Invalid input for circle type}, $t);


#
# Test of the "quote_identifier" database handle method
#

%quotetests = (
									q{0} => q{"0"},
									q{Ain't misbehaving } => q{"Ain't misbehaving "},
									NULL => q{"NULL"},
									"" => q{""}, ## no critic
							);
for (keys %quotetests) {
	$t=qq{DB handle method "quote_identifier" works with a value of "$_"};
	$result = $dbh->quote_identifier($_);
	is ($result, $quotetests{$_}, $t);
}
$t='DB handle method "quote_identifier" works with an undefined value';
is ($dbh->quote_identifier(undef), q{}, $t);

$t='DB handle method "quote_identifier" works with schemas';
is ($dbh->quote_identifier( undef, 'Her schema', 'My table' ), q{"Her schema"."My table"}, $t);



#
# Test of the "table_attributes" database handle method (deprecated)
#

# Because this function is deprecated and really just calling the column_info() 
# and primary_key() methods, we will do minimal testing.
$t='DB handle method "table_attributes" returns the expected fields';
$result = $dbh->func('dbd_pg_test', 'table_attributes');
$result = $result->[0];
@required =
	(qw(NAME TYPE SIZE NULLABLE DEFAULT CONSTRAINT PRIMARY_KEY REMARKS));
undef %missing;
for (@required) {
	$missing{$_}++ if ! exists $result->{$_};
}
is_deeply (\%missing, {}, $t);

#
# Test of the "pg_lo_*" database handle methods
#

$t='DB handle method "pg_lo_creat" returns a valid descriptor for reading';
$dbh->{AutoCommit}=1; $dbh->{AutoCommit}=0; ## Catch error where not in begin

my ($R,$W) = ($dbh->{pg_INV_READ}, $dbh->{pg_INV_WRITE});
my $RW = $R|$W;
my $object;

$t='DB handle method "pg_lo_creat" works with old-school dbh->func() method';
$object = $dbh->func($W, 'pg_lo_creat');
like ($object, qr/^\d+$/o, $t);
isnt ($object, 0, $t);

$t='DB handle method "pg_lo_creat" works with deprecated dbh->func(...lo_creat) method';
$object = $dbh->func($W, 'lo_creat');
like ($object, qr/^\d+$/o, $t);
isnt ($object, 0, $t);

$t='DB handle method "pg_lo_creat" returns a valid descriptor for writing';
$object = $dbh->pg_lo_creat($W);
like ($object, qr/^\d+$/o, $t);
isnt ($object, 0, $t);

$t='DB handle method "pg_lo_open" returns a valid descriptor for writing';
my $handle = $dbh->pg_lo_open($object, $W);
like ($handle, qr/^\d+$/o, $t);
isnt ($object, -1, $t);

$t='DB handle method "pg_lo_lseek" works when writing';
$result = $dbh->pg_lo_lseek($handle, 0, 0);
is ($result, 0, $t);
isnt ($object, -1, $t);

$t='DB handle method "pg_lo_write" works';
my $buf = 'tangelo mulberry passionfruit raspberry plantain' x 500;
$result = $dbh->pg_lo_write($handle, $buf, length($buf));
is ($result, length($buf), $t);
cmp_ok ($object, '>', 0, $t);

$t='DB handle method "pg_lo_close" works after write';
$result = $dbh->pg_lo_close($handle);
ok ($result, $t);

# Reopen for reading
$t='DB handle method "pg_lo_open" returns a valid descriptor for reading';
$handle = $dbh->pg_lo_open($object, $R);
like ($handle, qr/^\d+$/o, $t);
cmp_ok ($handle, 'eq', 0, $t);

$t='DB handle method "pg_lo_lseek" works when reading';
$result = $dbh->pg_lo_lseek($handle, 11, 0);
is ($result, 11, $t);

$t='DB handle method "pg_lo_tell" works';
$result = $dbh->pg_lo_tell($handle);
is ($result, 11, $t);

$t='DB handle method "pg_lo_read" read back the same data that was written';
$dbh->pg_lo_lseek($handle, 0, 0);
my ($buf2,$data) = ('','');
while ($dbh->pg_lo_read($handle, $data, 513)) {
	$buf2 .= $data;
}
is (length($buf), length($buf2), $t);

$t='DB handle method "pg_lo_close" works after read';
$result = $dbh->pg_lo_close($handle);
ok ($result, $t);

$t='DB handle method "pg_lo_unlink" works';
$result = $dbh->pg_lo_unlink($object);
is ($result, 1, $t);

$t='DB handle method "pg_lo_unlink" fails when called second time';
$result = $dbh->pg_lo_unlink($object);
ok (!$result, $t);
$dbh->rollback();

SKIP: {

	eval {
		require File::Temp;
	};
	$@ and skip ('Must have File::Temp to test pg_lo_import and pg_lo_export', 8);

	$t='DB handle method "pg_lo_import" works';
	my ($fh,$filename) = File::Temp::tmpnam();
	print $fh "abc\ndef";
	close $fh or warn 'Failed to close temporary file';
	$handle = $dbh->pg_lo_import($filename);
	my $objid = $handle;
	ok ($handle, $t);
	unlink $filename;

	$t='DB handle method "pg_lo_import" inserts correct data';
	$SQL = "SELECT data FROM pg_largeobject where loid = $handle";
	$info = $dbh->selectall_arrayref($SQL)->[0][0];
	is_deeply ($info, "abc\ndef", $t);

	$t='DB handle method "pg_lo_open" works after "pg_lo_insert"';
	$handle = $dbh->pg_lo_open($handle, $R);
	like ($handle, qr/^\d+$/o, $t);

	$t='DB handle method "pg_lo_read" returns correct data after "pg_lo_import"';
	$data = '';
	$result = $dbh->pg_lo_read($handle, $data, 100);
	is ($result, 7, $t);
	is ($data, "abc\ndef", $t);

	$t='DB handle method "pg_lo_export" works';
	($fh,$filename) = File::Temp::tmpnam();
	$result = $dbh->pg_lo_export($objid, $filename);
	ok (-e $filename, $t);
	seek($fh,0,1);
	seek($fh,0,0);
	$result = read $fh, $data, 10;
	is ($result, 7, $t);
	is ($data, "abc\ndef", $t);
	close $fh or warn 'Could not close tempfile';
	unlink $filename;
}

## Same pg_lo_* tests, but with AutoCommit on

$dbh->{AutoCommit}=1;

$t='DB handle method "pg_lo_creat" fails when AutoCommit on';
eval {
	$dbh->pg_lo_creat($W);
};
like ($@, qr{pg_lo_creat when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_open" fails with AutoCommit on';
eval {
	$dbh->pg_lo_open($object, $W);
};
like ($@, qr{pg_lo_open when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_read" fails with AutoCommit on';
eval {
	$dbh->pg_lo_read($object, $data, 0);
};
like ($@, qr{pg_lo_read when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_lseek" fails with AutoCommit on';
eval {
	$dbh->pg_lo_lseek($handle, 0, 0);
};
like ($@, qr{pg_lo_lseek when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_write" fails with AutoCommit on';
$buf = 'tangelo mulberry passionfruit raspberry plantain' x 500;
eval {
	$dbh->pg_lo_write($handle, $buf, length($buf));
};
like ($@, qr{pg_lo_write when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_close" fails with AutoCommit on';
eval {
	$dbh->pg_lo_close($handle);
};
like ($@, qr{pg_lo_close when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_tell" fails with AutoCommit on';
eval {
	$dbh->pg_lo_tell($handle);
};
like ($@, qr{pg_lo_tell when AutoCommit is on}, $t);

$t='DB handle method "pg_lo_unlink" fails with AutoCommit on';
eval {
	$dbh->pg_lo_unlink($object);
};
like ($@, qr{pg_lo_unlink when AutoCommit is on}, $t);


SKIP: {

	eval {
		require File::Temp;
	};
	$@ and skip ('Must have File::Temp to test pg_lo_import and pg_lo_export', 5);

	$t='DB handle method "pg_lo_import" works (AutoCommit on)';
	my ($fh,$filename) = File::Temp::tmpnam();
	print $fh "abc\ndef";
	close $fh or warn 'Failed to close temporary file';
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);

	$t='DB handle method "pg_lo_import" inserts correct data (AutoCommit on, begin_work not called)';
	$SQL = 'SELECT data FROM pg_largeobject where loid = ?';
	$sth = $dbh->prepare($SQL);
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, "abc\ndef", $t);

	$t='DB handle method "pg_lo_import" works (AutoCommit on, begin_work called, no command)';
	$dbh->begin_work();
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, "abc\ndef", $t);
	$dbh->rollback();

	$t='DB handle method "pg_lo_import" works (AutoCommit on, begin_work called, no command, rollback)';
	$dbh->begin_work();
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$dbh->rollback();
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, undef, $t);

	$t='DB handle method "pg_lo_import" works (AutoCommit on, begin_work called, second command)';
	$dbh->begin_work();
	$dbh->do('SELECT 123');
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, "abc\ndef", $t);
	$dbh->rollback();

	$t='DB handle method "pg_lo_import" works (AutoCommit on, begin_work called, second command, rollback)';
	$dbh->begin_work();
	$dbh->do('SELECT 123');
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$dbh->rollback();
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, undef, $t);

	$t='DB handle method "pg_lo_import" works (AutoCommit not on, no command)';
	$dbh->{AutoCommit} = 0;
	$dbh->commit();
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, "abc\ndef", $t);

	$t='DB handle method "pg_lo_import" works (AutoCommit not on, second command)';
	$dbh->rollback();
	$dbh->do('SELECT 123');
	$handle = $dbh->pg_lo_import($filename);
	ok ($handle, $t);
	$sth->execute($handle);
	$info = $sth->fetchall_arrayref()->[0][0];
	is_deeply ($info, "abc\ndef", $t);

	unlink $filename;
	$dbh->{AutoCommit} = 1;

	my $objid = $handle;
	$t='DB handle method "pg_lo_export" works (AutoCommit on)';
	($fh,$filename) = File::Temp::tmpnam();
	$result = $dbh->pg_lo_export($objid, $filename);
	ok (-e $filename, $t);
	seek($fh,0,1);
	seek($fh,0,0);
	$result = read $fh, $data, 10;
	is ($result, 7, $t);
	is ($data, "abc\ndef", $t);
	close $fh or warn 'Could not close tempfile';
	unlink $filename;
}
$dbh->{AutoCommit} = 0;

#
# Test of the "pg_notifies" database handle method
#

$t='DB handle method "pg_notifies" does not throw an error';
eval {
  $dbh->func('pg_notifies');
};
is ($@, q{}, $t);

$t='DB handle method "pg_notifies" (func) returns the correct values';
my $notify_name = 'dbdpg_notify_test';
my $pid = $dbh->selectall_arrayref('SELECT pg_backend_pid()')->[0][0];
$dbh->do("LISTEN $notify_name");
$dbh->do("NOTIFY $notify_name");
$dbh->commit();
$info = $dbh->func('pg_notifies');
is_deeply ($info, [$notify_name, $pid, ''], $t);

$t='DB handle method "pg_notifies" returns the correct values';
$dbh->do("NOTIFY $notify_name");
$dbh->commit();
$info = $dbh->pg_notifies;
is_deeply ($info, [$notify_name, $pid, ''], $t);

#
# Test of the "getfd" database handle method
#

$t='DB handle method "getfd" returns a number';
$result = $dbh->func('getfd');
like ($result, qr/^\d+$/, $t);

#
# Test of the "state" database handle method
#

$t='DB handle method "state" returns an empty string on success';
$result = $dbh->state();
is ($result, q{}, $t);

$t='DB handle method "state" returns a five-character code on error';
eval {
	$dbh->do('SELECT dbdpg_throws_an_error');
};
$result = $dbh->state();
like ($result, qr/^[A-Z0-9]{5}$/, $t);
$dbh->rollback();

#
# Test of the "private_attribute_info" database handle method
#

SKIP: {
	if ($DBI::VERSION < 1.54) {
		skip ('DBI must be at least version 1.54 to test private_attribute_info', 2);
	}

	$t='DB handle method "private_attribute_info" returns at least one record';
	my $private = $dbh->private_attribute_info();
	my ($valid,$invalid) = (0,0);
	for my $name (keys %$private) {
		$name =~ /^pg_\w+/ ? $valid++ : $invalid++;
	}
	ok ($valid >= 1, $t);

	$t='DB handle method "private_attribute_info" returns only internal names';
	is ($invalid, 0, $t);

}

#
# Test of the "clone" database handle method
#

$t='Database handle method "clone" does not throw an error';
my $dbh2;
eval { $dbh2 = $dbh->clone(); };
is ($@, q{}, $t);

$t='Database handle method "clone" returns a valid database handle';
eval {
	$dbh2->do('SELECT 123');
};
is ($@, q{}, $t);

$dbh2->disconnect();

#
# Test of the "ping" database handle method
#

$t='DB handle method "ping" returns 1 on an idle connection';
is ($dbh->ping(), 1, $t);

$t='DB handle method "ping" returns 3 for a good connection inside a transaction';
$dbh->do('SELECT 123');
$result = 3;
is ($result, $dbh->ping(), $t);

$t='DB handle method "ping" returns 1 on an idle connection';
$dbh->commit();
is ($dbh->ping(), 1, $t);

my $mtvar; ## This is an implicit test of getline: please leave this var undefined

$t='DB handle method "ping" returns 2 when in COPY IN state';
$dbh->do('COPY dbd_pg_test(id,pname) TO STDOUT');
{
	local $SIG{__WARN__} = sub {};
	$dbh->pg_getline($mtvar,100);
}
is ($dbh->ping(), 2, $t);
1 while $dbh->pg_getline($mtvar,1000);

$t='DB handle method "ping" returns 3 for a good connection inside a transaction';
$dbh->do('SELECT 123');
is ($dbh->ping(), 3, $t);

$t='DB handle method "ping" returns a 4 when inside a failed transaction';
eval {
	$dbh->do('DBD::Pg creating an invalid command for testing');
};
is ($dbh->ping(), 4, $t);

$t='DB handle method "ping" fails (returns 0) on a disconnected handle';
$dbh->disconnect();
is ($dbh->ping(), 0, $t);

$t='Able to reconnect to the database after disconnect';
$dbh = connect_database({nosetup => 1});
isnt ($dbh, undef, $t);

#
# Test of the "pg_ping" database handle method
#

$t='DB handle method "pg_ping" returns 1 on an idle connection';
is ($dbh->pg_ping(), 1, $t);

$t='DB handle method "pg_ping" returns 3 for a good connection inside a transaction';
$dbh->do('SELECT 123');
is ($dbh->pg_ping(), 3, $t);

$t='DB handle method "pg_ping" returns 1 on an idle connection';
$dbh->commit();
is ($dbh->pg_ping(), 1, $t);

$t='DB handle method "pg_ping" returns 2 when in COPY IN state';
$dbh->do('COPY dbd_pg_test(id,pname) TO STDOUT');
$dbh->pg_getline($mtvar,100);
is ($dbh->pg_ping(), 2, $t);

$t='DB handle method "pg_ping" returns 2 immediately after COPY IN state';
1 while $dbh->pg_getline($mtvar,1000);
is ($dbh->pg_ping(), 2, $t);

$t='DB handle method "pg_ping" returns 3 for a good connection inside a transaction';
$dbh->do('SELECT 123');
is ($dbh->pg_ping(), 3, $t);

$t='DB handle method "pg_ping" returns a 4 when inside a failed transaction';
eval {
	$dbh->do('DBD::Pg creating an invalid command for testing');
};
is ($dbh->pg_ping(), 4, $t);

$t='DB handle method "pg_ping" fails (returns 0) on a disconnected handle';
cleanup_database($dbh,'test');
$dbh->disconnect();
is ($dbh->pg_ping(), -1, $t);
