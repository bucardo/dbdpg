#!perl -w

# Test of the database handle methods
# The following methods are *not* (explicitly) tested here:
# "clone"
# "data_sources"
# "disconnect"
# "take_imp_data"
# "lo_import"
# "lo_export"
# "pg_savepoint", "pg_release", "pg_rollback_to"
# "pg_putline", "pg_getline", "pg_endcopy"

use Test::More;
use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 203;
}
else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for database handle method testing");

my $schema = '';
my $got73 = DBD::Pg::_pg_use_catalog($dbh);
if ($got73) {
	$schema = exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public';
	$dbh->do("SET search_path TO " . $dbh->quote_identifier($schema));
}

my ($SQL, $sth, $result, @result, $expected, $warning, $rows, $t);

# Quick simple "tests"

$dbh->do(""); ## This used to break, so we keep it as a test...
$SQL = "SELECT '2529DF6AB8F79407E94445B4BC9B906714964AC8' FROM dbd_pg_test WHERE id=?";
$sth = $dbh->prepare($SQL);
$sth->finish();
$sth = $dbh->prepare_cached($SQL);
$sth->finish();

# Populate the testing table for later use

$dbh->do("DELETE FROM dbd_pg_test");
$SQL = "INSERT INTO dbd_pg_test(id,val) VALUES (?,?)";

$sth = $dbh->prepare($SQL);
$sth->bind_param(1, 1, SQL_INTEGER);
$sth->execute(10,'Roseapple');
$sth->execute(11,'Pineapple');
$sth->execute(12,'Kiwi');

#
# Test of the "last_insert_id" database handle method
#

$dbh->commit();
eval {
	$result = $dbh->last_insert_id(undef,undef,undef,undef);
};
ok( $@, 'DB handle method "last_insert_id" given an error when no arguments are given');

eval {
	$result = $dbh->last_insert_id(undef,undef,undef,undef,{sequence=>'dbd_pg_nonexistentsequence_test'});
};
ok( $@, 'DB handle method "last_insert_id" fails when given a non-existent sequence');
$dbh->rollback();

eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef);
};
ok( $@, 'DB handle method "last_insert_id" fails when given a non-existent table');
$dbh->rollback();

eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_nonexistenttable_test',undef,{sequence=>'dbd_pg_sequence'});
};
ok( ! $@, 'DB handle method "last_insert_id" works when given a valid sequence and an invalid table');
like( $result, qr{^\d+$}, 'DB handle method "last_insert_id" returns a numeric value');

eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef);
};
ok( ! $@, 'DB handle method "last_insert_id" works when given a valid table');

eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef,'');
};
ok( ! $@, 'DB handle method "last_insert_id" works when given an empty attrib');

eval {
	$result = $dbh->last_insert_id(undef,undef,'dbd_pg_test',undef);
};
ok( ! $@, 'DB handle method "last_insert_id" works when called twice (cached) given a valid table');

#$dbh->do("DROP SCHEMA IF EXISTS dbd_pg_testli CASCADE");
$dbh->do("CREATE SCHEMA dbd_pg_testli");
$dbh->do("CREATE SEQUENCE dbd_pg_testli.dbd_pg_testseq");
$dbh->{Warn}=0;
$dbh->do("CREATE TABLE dbd_pg_testli.dbd_pg_litest(a INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('dbd_pg_testli.dbd_pg_testseq'))");
$dbh->{Warn}=1;
$dbh->do("INSERT INTO dbd_pg_testli.dbd_pg_litest DEFAULT VALUES");
eval {
	$result = $dbh->last_insert_id(undef,'dbd_pg_testli','dbd_pg_litest',undef);
};
is ($@, q{}, 'DB handle method "last_insert_id" works when called with a schema not in the search path');
is ($result, 1, qq{Got 1});
$dbh->commit();


$t=qq{ DB handle method "last_insert_id" fails when the sequence name is changed and cache is used};
$dbh->do("ALTER SEQUENCE dbd_pg_testli.dbd_pg_testseq RENAME TO dbd_pg_testseq2");
$dbh->commit();
eval {
	$dbh->last_insert_id(undef,'dbd_pg_testli','dbd_pg_litest',undef);
};
like ($@, qr{last_insert_id}, $t);
$dbh->rollback();

$t=qq{ DB handle method "last_insert_id" fails when the sequence name is changed and cache is turned off};
$dbh->commit();
eval {
	$dbh->last_insert_id(undef,'dbd_pg_testli','dbd_pg_litest',undef, {pg_cache=>0});
};
is ($@, q{}, $t);
is ($result, 1, qq{Got 1});


$dbh->do("DROP TABLE dbd_pg_testli.dbd_pg_litest CASCADE");
$dbh->do("DROP SEQUENCE dbd_pg_testli.dbd_pg_testseq2");
$dbh->do("DROP SCHEMA dbd_pg_testli CASCADE");

#
# Test of the "selectrow_array" database handle method
#

$SQL = "SELECT id FROM dbd_pg_test ORDER BY id";
@result = $dbh->selectrow_array($SQL);
$expected = [10];
is_deeply(\@result, $expected, 'DB handle method "selectrow_array" works');

#
# Test of the "selectrow_arrayref" database handle method
#

$result = $dbh->selectrow_arrayref($SQL);
is_deeply($result, $expected, 'DB handle method "selectrow_arrayref" works');

$sth = $dbh->prepare($SQL);
$result = $dbh->selectrow_arrayref($sth);
is_deeply($result, $expected, 'DB handle method "selectrow_arrayref" works with a prepared statement handle');

#
# Test of the "selectrow_hashref" database handle method
#

$result = $dbh->selectrow_hashref($SQL);
$expected = {id => 10};
is_deeply($result, $expected, 'DB handle method "selectrow_hashref" works');

$sth = $dbh->prepare($SQL);
$result = $dbh->selectrow_hashref($sth);
is_deeply($result, $expected, 'DB handle method "selectrow_hashref" works with a prepared statement handle');

#
# Test of the "selectall_arrayref" database handle method
#

$result = $dbh->selectall_arrayref($SQL);
$expected = [[10],[11],[12]];
is_deeply($result, $expected, 'DB handle method "selectall_arrayref" works');

$sth = $dbh->prepare($SQL);
$result = $dbh->selectall_arrayref($sth);
is_deeply($result, $expected, 'DB handle method "selectall_arrayref" works with a prepared statement handle');

$result = $dbh->selectall_arrayref($SQL, {MaxRows => 2});
$expected = [[10],[11]];
is_deeply($result, $expected, 'DB handle method "selectall_arrayref" works with the MaxRows attribute');

$SQL = "SELECT id, val FROM dbd_pg_test ORDER BY id";
$result = $dbh->selectall_arrayref($SQL, {Slice => [1]});
$expected = [['Roseapple'],['Pineapple'],['Kiwi']];
is_deeply($result, $expected, 'DB handle method "selectall_arrayref" works with the Slice attribute');

#
# Test of the "selectall_hashref" database handle method
#

$result = $dbh->selectall_hashref($SQL,'id');
$expected = {10=>{id =>10,val=>'Roseapple'},11=>{id=>11,val=>'Pineapple'},12=>{id=>12,val=>'Kiwi'}};
is_deeply($result, $expected, 'DB handle method "selectall_hashref" works');

$sth = $dbh->prepare($SQL);
$result = $dbh->selectall_hashref($sth,'id');
is_deeply($result, $expected, 'DB handle method "selectall_hashref" works with a prepared statement handle');

#
# Test of the "selectcol_arrayref" database handle method
#

$result = $dbh->selectcol_arrayref($SQL);
$expected = [10,11,12];
is_deeply($result, $expected, 'DB handle method "selectcol_arrayref" works');

$result = $dbh->selectcol_arrayref($sth);
is_deeply($result, $expected, 'DB handle method "selectcol_arrayref" works with a prepared statement handle');

$result = $dbh->selectcol_arrayref($SQL, {Columns=>[2,1]});
$expected = ['Roseapple',10,'Pineapple',11,'Kiwi',12];
is_deeply($result, $expected, 'DB handle method "selectcol_arrayref" works with the Columns attribute');

$result = $dbh->selectcol_arrayref($SQL, {Columns=>[2], MaxRows => 1});
$expected = ['Roseapple'];
is_deeply($result, $expected, 'DB handle method "selectcol_arrayref" works with the MaxRows attribute');

#
# Test of the "commit" and "rollback" database handle methods
#

{
	local $SIG{__WARN__} = sub { $warning = shift; };
	$dbh->{AutoCommit}=0;

	$warning="";
	$dbh->commit();
	ok( ! length $warning, 'DB handle method "commit" gives no warning when AutoCommit is off');
	$warning="";
	$dbh->rollback();
	ok( ! length $warning, 'DB handle method "rollback" gives no warning when AutoCommit is off');

	ok( $dbh->commit, 'DB handle method "commit" returns true');
	ok( $dbh->rollback, 'DB handle method "rollback" returns true');

	$dbh->{AutoCommit}=1;
	$warning="";
	$dbh->commit();
	ok( length $warning, 'DB handle method "commit" gives a warning when AutoCommit is on');
	$warning="";
	$dbh->rollback();
	ok( length $warning, 'DB handle method "rollback" gives a warning when AutoCommit is on');


}

#
# Test of the "begin_work" database handle method
#

$dbh->{AutoCommit}=0;
eval {
	$dbh->begin_work();
};
ok( $@, 'DB handle method "begin_work" gives a warning when AutoCommit is on');

$dbh->{AutoCommit}=1;
eval {
	$dbh->begin_work();
};
ok( !$@, 'DB handle method "begin_work" gives no warning when AutoCommit is off');
ok( !$dbh->{AutoCommit}, 'DB handle method "begin_work" sets AutoCommit to off');
$dbh->commit();
ok( $dbh->{AutoCommit}, 'DB handle method "commit" after "begin_work" sets AutoCommit to on');

$dbh->{AutoCommit}=1;
eval {
	$dbh->begin_work();
};
ok( !$@, 'DB handle method "begin_work" gives no warning when AutoCommit is off');
ok( !$dbh->{AutoCommit}, 'DB handle method "begin_work" sets AutoCommit to off');
$dbh->rollback();
ok( $dbh->{AutoCommit}, 'DB handle method "rollback" after "begin_work" sets AutoCommit to on');

$dbh->{AutoCommit}=0;

#
# Test of the "get_info" database handle method
#

eval {
  $dbh->get_info();
};
ok ($@, 'DB handle method "get_info" with no arguments gives an error');

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
	my $back = $dbh->get_info($_);
	ok( defined $back, qq{DB handle method "get_info" works with a value of "$_"});
	my $forth = $dbh->get_info($get_info{$_});
	ok( defined $forth, qq{DB handle method "get_info" works with a value of "$get_info{$_}"});
	is( $back, $forth, qq{DB handle method "get_info" returned matching values});
}

# Make sure odbcversion looks normal
my $odbcversion = $dbh->get_info(18);
like( $odbcversion, qr{^([1-9]\d|\d[1-9])\.\d\d\.\d\d00$}, qq{DB handle method "get_info" returns a valid looking ODBCVERSION string});

# Testing max connections is good as this info is dynamic
my $maxcon = $dbh->get_info(0);
like( $maxcon, qr{^\d+$}, qq{DB handle method "get_info" returns a number for SQL_MAX_DRIVER_CONNECTIONS});

#
# Test of the "table_info" database handle method
#

$sth = $dbh->table_info('', '', 'dbd_pg_test', '');
my $number = $sth->rows();
ok( $number, 'DB handle method "table_info" works when called with undef arguments');

# Check required minimum fields
$result = $sth->fetchall_arrayref({});
my @required = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME TABLE_TYPE REMARKS));
my %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply( \%missing, {}, 'DB handle method "table_info" returns fields required by DBI');

## Check some of the returned fields:
$result = $result->[0];
is( $result->{TABLE_CAT}, undef, 'DB handle method "table_info" returns proper TABLE_CAT');
is( $result->{TABLE_NAME}, 'dbd_pg_test', 'DB handle method "table_info" returns proper TABLE_NAME');
is( $result->{TABLE_TYPE}, 'TABLE', 'DB handle method "table_info" returns proper TABLE_TYPE');

$sth = $dbh->table_info(undef,undef,undef,"TABLE,VIEW");
$number = $sth->rows();
cmp_ok( $number, '>', 1, qq{DB handle method "table_info" returns correct number of rows when given a 'TABLE,VIEW' type argument});

$sth = $dbh->table_info(undef,undef,undef,"DUMMY");
$rows = $sth->rows();
is( $rows, $number, 'DB handle method "table_info" returns correct number of rows when given an invalid type argument');

$sth = $dbh->table_info(undef,undef,undef,"VIEW");
$rows = $sth->rows();
cmp_ok( $rows, '<', $number, qq{DB handle method "table_info" returns correct number of rows when given a 'VIEW' type argument});

$sth = $dbh->table_info(undef,undef,undef,"TABLE");
$rows = $sth->rows();
cmp_ok( $rows, '<', $number, qq{DB handle method "table_info" returns correct number of rows when given a 'TABLE' type argument});

# Test listing catalog names
$sth = $dbh->table_info('%', '', '');
ok( $sth, 'DB handle method "table_info" works when called with a catalog of %');

# Test listing schema names
$sth = $dbh->table_info('', '%', '');
ok( $sth, 'DB handle method "table_info" works when called with a schema of %');

# Test listing table types
$sth = $dbh->table_info('', '', '', '%');
ok( $sth, 'DB handle method "table_info" works when called with a type of %');

#
# Test of the "column_info" database handle method
#

# Check required minimum fields
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
is_deeply( \%missing, {}, 'DB handle method "column_info" returns fields required by DBI');

# Check that pg_constraint was populated
$result = $result->[0];
like( $result->{pg_constraint}, qr/score/, qq{DB handle method "column info" 'pg_constraint' returns a value for constrained columns});

# Check that it is not populated for non-constrained columns
$sth = $dbh->column_info('','','dbd_pg_test','id');
$result = $sth->fetchall_arrayref({})->[0];
is( $result->{pg_constraint}, undef, qq{DB handle method "column info" 'pg_constraint' returns undef for non-constrained columns});

# Check the rest of the custom "pg" columns
is( $result->{pg_type}, 'integer', qq{DB handle method "column_info" returns good value for 'pg_type'});

## Check some of the returned fields:
is( $result->{TABLE_CAT}, undef, 'DB handle method "column_info" returns proper TABLE_CAT');
is( $result->{TABLE_NAME}, 'dbd_pg_test', 'DB handle method "column_info returns proper TABLE_NAME');
is( $result->{COLUMN_NAME}, 'id', 'DB handle method "column_info" returns proper COLUMN_NAME');
is( $result->{DATA_TYPE}, 4, 'DB handle method "column_info" returns proper DATA_TYPE');
is( $result->{COLUMN_SIZE}, 4, 'DB handle method "column_info" returns proper COLUMN_SIZE');
is( $result->{NULLABLE}, '0', 'DB handle method "column_info" returns proper NULLABLE');
is( $result->{REMARKS}, 'Bob is your uncle', 'DB handle method "column_info" returns proper REMARKS');
is( $result->{COLUMN_DEF}, undef, 'DB handle method "column_info" returns proper COLUMN_DEF');
is( $result->{ORDINAL_POSITION}, 1, 'DB handle method "column_info" returns proper ORDINAL_POSITION');
is( $result->{IS_NULLABLE}, 'NO', 'DB handle method "column_info" returns proper IS_NULLABLE');
is( $result->{pg_type}, 'integer', 'DB handle method "column_info" returns proper pg_type');

#
# Test of the "primary_key_info" database handle method
#

# Check required minimum fields
$sth = $dbh->primary_key_info('','','dbd_pg_test');
$result = $sth->fetchall_arrayref({});
@required = 
	(qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME KEY_SEQ 
			PK_NAME DATA_TYPE));
undef %missing;
for my $r (@$result) {
	for (@required) {
		$missing{$_}++ if ! exists $r->{$_};
	}
}
is_deeply( \%missing, {}, 'DB handle method "primary_key_info" returns required fields');

## Check some of the returned fields:
$result = $result->[0];
is( $result->{TABLE_CAT}, undef, 'DB handle method "primary_key_info" returns proper TABLE_CAT');
is( $result->{TABLE_NAME}, 'dbd_pg_test', 'DB handle method "primary_key_info" returns proper TABLE_NAME');
is( $result->{COLUMN_NAME}, 'id', 'DB handle method "primary_key_info" returns proper COLUMN_NAME');
cmp_ok( $result->{KEY_SEQ}, '>=', 1, 'DB handle method "primary_key_info" returns proper KEY_SEQ');

#
# Test of the "primary_key" database handle method
#

@result = $dbh->primary_key('', '', 'dbd_pg_test');
$expected = ['id'];
is_deeply( \@result, $expected, 'DB handle method "primary_key" works');

@result = $dbh->primary_key('', '', 'dbd_pg_test_do_not_create_this_table');
$expected = [];
is_deeply( \@result, $expected, 'DB handle method "primary_key" returns empty list for invalid table');

#
# Test of the "statistics_info" database handle method
#

SKIP: {

	$DBI::VERSION >= 1.52
		or skip	'DBI must be at least version 1.52 to test the database handle method "statistics_info"', 10;

	$sth = $dbh->statistics_info(undef,undef,undef,undef,undef);
	is ($sth, undef, 'DB handle method "statistics_info" returns undef: no table');

	# Drop any tables that may exist
	my $fktables = join "," => map { "'dbd_pg_test$_'" } (1..3);
	$SQL = "SELECT relname FROM pg_catalog.pg_class WHERE relkind='r' AND relname IN ($fktables)";
	{
		local $SIG{__WARN__} = sub {};
		for (@{$dbh->selectall_arrayref($SQL)}) {
			$dbh->do("DROP TABLE $_->[0] CASCADE");
		}
	}

	## Invalid table
	$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test9',undef,undef);
	is ($sth, undef, 'DB handle method "statistics_info" returns undef: bad table');

	## Create some tables with various indexes
	{
		local $SIG{__WARN__} = sub {};
		$dbh->do("CREATE TABLE dbd_pg_test1 (a INT, b INT NOT NULL, c INT NOT NULL, ".
				 "CONSTRAINT dbd_pg_test1_pk PRIMARY KEY (a))");
		$dbh->do("ALTER TABLE dbd_pg_test1 ADD CONSTRAINT dbd_pg_test1_uc1 UNIQUE (b)");
		$dbh->do("CREATE UNIQUE INDEX dbd_pg_test1_index_c ON dbd_pg_test1(c)");
		$dbh->do("CREATE TABLE dbd_pg_test2 (a INT, b INT, c INT, PRIMARY KEY(a,b), UNIQUE(b,c))");
		$dbh->do("CREATE INDEX dbd_pg_test2_skipme ON dbd_pg_test2(c,(a+b))");
		$dbh->do("CREATE TABLE dbd_pg_test3 (a INT, b INT, c INT, PRIMARY KEY(a)) WITH OIDS");
		$dbh->do("CREATE UNIQUE INDEX dbd_pg_test3_index_b ON dbd_pg_test3(b)");
		$dbh->do("CREATE INDEX dbd_pg_test3_index_c ON dbd_pg_test3 USING hash(c)");
		$dbh->do("CREATE INDEX dbd_pg_test3_oid ON dbd_pg_test3(oid)");
		$dbh->do("CREATE UNIQUE INDEX dbd_pg_test3_pred ON dbd_pg_test3(c) WHERE c > 0 AND c < 45");
		$dbh->commit();
	}

	my $correct_stats = {
	one => [
	[ undef, 'public', 'dbd_pg_test1', undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, 'public', 'dbd_pg_test1', '0', undef, 'dbd_pg_test1_index_c', 'btree',  1, 'c', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test1', '0', undef, 'dbd_pg_test1_pk',      'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test1', '0', undef, 'dbd_pg_test1_uc1',     'btree',  1, 'b', 'A', '0', '1', undef ],
	],
	two => [
	[ undef, 'public', 'dbd_pg_test2', undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, 'public', 'dbd_pg_test2', '0', undef, 'dbd_pg_test2_b_key',   'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test2', '0', undef, 'dbd_pg_test2_b_key',   'btree',  2, 'c', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test2', '0', undef, 'dbd_pg_test2_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test2', '0', undef, 'dbd_pg_test2_pkey',    'btree',  2, 'b', 'A', '0', '1', undef ],
	],
	three => [
	[ undef, 'public', 'dbd_pg_test3', undef, undef, undef, 'table', undef, undef, undef, '0', '0', undef ],
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_index_b', 'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_pred',    'btree',  1, 'c', 'A', '0', '1', '((c > 0) AND (c < 45))' ],
	[ undef, 'public', 'dbd_pg_test3', '1', undef, 'dbd_pg_test3_oid',     'btree',  1, 'oid', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test3', '1', undef, 'dbd_pg_test3_index_c', 'hashed', 1, 'c', 'A', '0', '4', undef ],
],
	three_uo => [
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_index_b', 'btree',  1, 'b', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_pkey',    'btree',  1, 'a', 'A', '0', '1', undef ],
	[ undef, 'public', 'dbd_pg_test3', '0', undef, 'dbd_pg_test3_pred',    'btree',  1, 'c', 'A', '0', '1', '((c > 0) AND (c < 45))' ],
	],
	};

	if(!$got73) { # wipe out the schema names in the expected results above
		for my $subset (values %$correct_stats) {
			for (@$subset) {
				$_->[1] = undef;
			}
		}
	}

  SKIP: {
		skip qq{Cannot test statistics_info with schema arg on pre-7.3 servers.}, 4
			if ! $got73;

        my $stats;

		$sth = $dbh->statistics_info(undef,'public','dbd_pg_test1',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{one}, 'Correct stats output for public.dbd_pg_test1');

		$sth = $dbh->statistics_info(undef,'public','dbd_pg_test2',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{two}, 'Correct stats output for public.dbd_pg_test2');

		$sth = $dbh->statistics_info(undef,'public','dbd_pg_test3',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{three}, 'Correct stats output for public.dbd_pg_test3');

		$sth = $dbh->statistics_info(undef,'public','dbd_pg_test3',1,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{three_uo}, 'Correct stats output for public.dbd_pg_test3 (unique only)');
	}

	{
        my $stats;

		$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test1',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{one}, 'Correct stats output for dbd_pg_test1');

		$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test2',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{two}, 'Correct stats output for dbd_pg_test2');

		$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test3',undef,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{three}, 'Correct stats output for dbd_pg_test3');

		$sth = $dbh->statistics_info(undef,undef,'dbd_pg_test3',1,undef);
        $stats = $sth->fetchall_arrayref;
		is_deeply($stats, $correct_stats->{three_uo}, 'Correct stats output for dbd_pg_test3 (unique only)');
	}

	# Clean everything up
	$dbh->do("DROP TABLE dbd_pg_test3");
	$dbh->do("DROP TABLE dbd_pg_test2");
	$dbh->do("DROP TABLE dbd_pg_test1");

} ## end of statistics_info tests


#
# Test of the "foreign_key_info" database handle method
#

## Neither pktable nor fktable specified
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,undef);
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: no pk / no fk');

## All foreign_key_info tests are meaningless for old servers
if (! $got73) {
 SKIP: {
		skip qq{Cannot test DB handle method "foreign_key_info" on pre-7.3 servers.}, 16;
	}
}
else {

# Drop any tables that may exist
my $fktables = join "," => map { "'dbd_pg_test$_'" } (1..3);
$SQL = "SELECT relname FROM pg_catalog.pg_class WHERE relkind='r' AND relname IN ($fktables)";
{
	local $SIG{__WARN__} = sub {};
	for (@{$dbh->selectall_arrayref($SQL)}) {
		$dbh->do("DROP TABLE $_->[0] CASCADE");
	}
}
## Invalid primary table
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,undef);
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: bad pk / no fk');

## Invalid foreign table
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,'dbd_pg_test9');
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: no pk / bad fk');

## Both primary and foreign are invalid
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,'dbd_pg_test9');
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: bad fk / bad fk');

## Create a pk table
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("CREATE TABLE dbd_pg_test1 (a INT, b INT NOT NULL, c INT NOT NULL, ".
					 "CONSTRAINT dbd_pg_test1_pk PRIMARY KEY (a))");
	$dbh->do("ALTER TABLE dbd_pg_test1 ADD CONSTRAINT dbd_pg_test1_uc1 UNIQUE (b)");
	$dbh->do("CREATE UNIQUE INDEX dbd_pg_test1_index_c ON dbd_pg_test1(c)");
	$dbh->commit();
}

## Good primary with no foreign keys
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: good pk (but unreferenced)');

## Create a simple foreign key table
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("CREATE TABLE dbd_pg_test2 (f1 INT PRIMARY KEY, f2 INT NOT NULL, f3 INT NOT NULL)");
	$dbh->do("ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk1 FOREIGN KEY(f2) REFERENCES dbd_pg_test1(a)");
	$dbh->commit();
}

## Bad primary with good foreign
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test9',undef,undef,'dbd_pg_test2');
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: bad pk / good fk');

## Good primary, good foreign, bad schemas
my $testschema = "dbd_pg_test_badschema11";
$sth = $dbh->foreign_key_info(undef,$testschema,'dbd_pg_test1',undef,undef,'dbd_pg_test2');
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: good pk / good fk / bad pk schema');
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,$testschema,'dbd_pg_test2');
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: good pk / good fk / bad fk schema');

## Good primary
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
$result = $sth->fetchall_arrayref({});

# Check required minimum fields
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
is_deeply( \%missing, {}, 'DB handle method "foreign_key_info" returns fields required by DBI');

## Good primary
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk1 = [
					 undef, ## Catalog
					 $schema, ## Schema
					 'dbd_pg_test1', ## Table
					 'a', ## Column
					 undef, ## FK Catalog
					 $schema, ## FK Schema
					 'dbd_pg_test2', ## FK Table
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
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good pk');

## Same with explicit table
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,'dbd_pg_test2');
$result = $sth->fetchall_arrayref();
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good pk / good fk');

## Foreign table only
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,'dbd_pg_test2');
$result = $sth->fetchall_arrayref();
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good fk');

## Add a foreign key to an explicit unique constraint
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk2 FOREIGN KEY (f3) ".
					 "REFERENCES dbd_pg_test1(b) ON DELETE SET NULL ON UPDATE CASCADE");
}
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk2 = [
					 undef,
					 $schema,
					 'dbd_pg_test1',
					 'b',
					 undef,
					 $schema,
					 'dbd_pg_test2',
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
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good pk / explicit fk');

## Add a foreign key to an implicit unique constraint (a unique index on a column)
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_aafk3 FOREIGN KEY (f3) ".
					 "REFERENCES dbd_pg_test1(c) ON DELETE RESTRICT ON UPDATE SET DEFAULT");
}
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk3 = [
					 undef,
					 $schema,
					 'dbd_pg_test1',
					 'c',
					 undef,
					 $schema,
					 'dbd_pg_test2',
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
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good pk / implicit fk');

## Create another foreign key table to point to the first (primary) table
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("CREATE TABLE dbd_pg_test3 (ff1 INT NOT NULL)");
	$dbh->do("ALTER TABLE dbd_pg_test3 ADD CONSTRAINT dbd_pg_test3_fk1 FOREIGN KEY(ff1) REFERENCES dbd_pg_test1(a)");
	$dbh->commit();
}

$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,undef);
$result = $sth->fetchall_arrayref();
my $fk4 = [
					 undef,
					 $schema,
					 'dbd_pg_test1',
					 'a',
					 undef,
					 $schema,
					 'dbd_pg_test3',
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
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for multiple fks');

## Test that explicit naming two tables brings back only those tables
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,'dbd_pg_test3');
$result = $sth->fetchall_arrayref();
$expected = [$fk4];
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for good pk / good fk (only)');

## Multi-column madness
{
	local $SIG{__WARN__} = sub {};
	$dbh->do("ALTER TABLE dbd_pg_test1 ADD CONSTRAINT dbd_pg_test1_uc2 UNIQUE (b,c,a)");
	$dbh->do("ALTER TABLE dbd_pg_test2 ADD CONSTRAINT dbd_pg_test2_fk4 " . 
					 "FOREIGN KEY (f1,f3,f2) REFERENCES dbd_pg_test1(c,a,b)");
}

$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test1',undef,undef,'dbd_pg_test2');
$result = $sth->fetchall_arrayref();
## "dbd_pg_test2_fk4" FOREIGN KEY (f1, f3, f2) REFERENCES dbd_pg_test1(c, a, b)
my $fk5 = [
					 undef,
					 $schema,
					 'dbd_pg_test1',
					 'c',
					 undef,
					 $schema,
					 'dbd_pg_test2',
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
is_deeply ($result, $expected, 'DB handle method "foreign_key_info" works for multi-column keys');

# Clean everything up
{
	$dbh->do("DROP TABLE dbd_pg_test3");
	$dbh->do("DROP TABLE dbd_pg_test2");
	$dbh->do("DROP TABLE dbd_pg_test1");
}

} # end giant foreign_key_info bypass

#
# Test of the "tables" database handle method
#

@result = $dbh->tables('', '', 'dbd_pg_test', '');
like( $result[0], qr/dbd_pg_test/, 'DB handle method "tables" works');

@result = $dbh->tables('', '', 'dbd_pg_test', '', {pg_noprefix => 1});
is( $result[0], 'dbd_pg_test', 'DB handle method "tables" works with a "pg_noprefix" attribute');

#
# Test of the "type_info_all" database handle method
#

$result = $dbh->type_info_all();

# Quick check that the structure looks correct
my $badresult="";
if (ref $result eq "ARRAY") {
	my $index = $result->[0];
	if (ref $index ne "HASH") {
		$badresult = "First element in array not a hash ref";
	}
	else {
		for (qw(TYPE_NAME DATA_TYPE CASE_SENSITIVE)) {
			$badresult = "Field $_ missing" if !exists $index->{$_};
		}
	}
}
else {
	$badresult = "Array reference not returned";
}
diag "type_info_all problem: $badresult" if $badresult;
ok ( !$badresult, 'DB handle method "type_info_all" returns a valid structure');

#
# Test of the "type_info" database handle method
#

# Check required minimum fields
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
is_deeply( \%missing, {}, 'DB handle method "type_info" returns fields required by DBI');

#
# Test of the "quote" database handle method
#

my %quotetests = (
	q{0} => q{'0'},
	q{Ain't misbehaving } => q{'Ain''t misbehaving '},
	NULL => q{'NULL'},
	"" => q{''},
);

for (keys %quotetests) {
	$result = $dbh->quote($_);
	is( $result, $quotetests{$_}, qq{DB handle method "quote" works with a value of "$_"});
}

## Test timestamp - should quote as a string
my $tstype = 93;
my $testtime = "2006-01-28 11:12:13";
is( $dbh->quote( $testtime, $tstype ), qq{'$testtime'}, qq{DB handle method "quote" work on timestamp});

my $foo;
{
	no warnings; ## Perl does not like undef args
	is( $dbh->quote($foo), q{NULL}, 'DB handle method "quote" works with an undefined value');
}
is( $dbh->quote(1, 4), 1, 'DB handle method "quote" works with a supplied data type argument');

#
# Test various quote types
#


## Points
eval { $result = $dbh->quote(q{123,456}, { pg_type => PG_POINT }); };
ok( !$@, 'DB handle method "quote" works with type PG_POINT');
is( $result, q{'123,456'}, 'DB handle method "quote" returns correct value for type PG_POINT');
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_POINT }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_POINT string');
eval { $result = $dbh->quote(q{A123,456}, { pg_type => PG_POINT }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_POINT string');

## Lines and line segments
eval { $result = $dbh->quote(q{123,456}, { pg_type => PG_LINE }); };
ok( !$@, 'DB handle method "quote" works with valid PG_LINE string');
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_LINE }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_LINE string');
eval { $result = $dbh->quote(q{<123,456}, { pg_type => PG_LINE }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_LINE string');
eval { $result = $dbh->quote(q{[123,456]}, { pg_type => PG_LSEG }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_LSEG string');
eval { $result = $dbh->quote(q{[123,456}, { pg_type => PG_LSEG }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_LSEG string');

## Boxes
eval { $result = $dbh->quote(q{1,2,3,4}, { pg_type => PG_BOX }); };
ok( !$@, 'DB handle method "quote" works with valid PG_BOX string');
eval { $result = $dbh->quote(q{[1,2,3,4]}, { pg_type => PG_BOX }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_BOX string');
eval { $result = $dbh->quote(q{1,2,3,4,cheese}, { pg_type => PG_BOX }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_BOX string');

## Paths - can have optional square brackets
eval { $result = $dbh->quote(q{[(1,2),(3,4)]}, { pg_type => PG_PATH }); };
ok( !$@, 'DB handle method "quote" works with valid PG_PATH string');
is( $result, q{'[(1,2),(3,4)]'}, 'DB handle method "quote" returns correct value for type PG_PATH');
eval { $result = $dbh->quote(q{<(1,2),(3,4)>}, { pg_type => PG_PATH }); };
like( $@, qr{Invalid input for geometric path type}, 'DB handle method "quote" fails with invalid PG_PATH string');
eval { $result = $dbh->quote(q{<1,2,3,4>}, { pg_type => PG_PATH }); };
like( $@, qr{Invalid input for geometric path type}, 'DB handle method "quote" fails with invalid PG_PATH string');

## Polygons
eval { $result = $dbh->quote(q{1,2,3,4}, { pg_type => PG_POLYGON }); };
ok( !$@, 'DB handle method "quote" works with valid PG_POLYGON string');
eval { $result = $dbh->quote(q{[1,2,3,4]}, { pg_type => PG_POLYGON }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_POLYGON string');
eval { $result = $dbh->quote(q{1,2,3,4,cheese}, { pg_type => PG_POLYGON }); };
like( $@, qr{Invalid input for geometric type}, 'DB handle method "quote" fails with invalid PG_POLYGON string');

## Circles - can have optional angle brackets
eval { $result = $dbh->quote(q{<(1,2,3)>}, { pg_type => PG_CIRCLE }); };
ok( !$@, 'DB handle method "quote" works with valid PG_CIRCLE string');
is( $result, q{'<(1,2,3)>'}, 'DB handle method "quote" returns correct value for type PG_CIRCLE');
eval { $result = $dbh->quote(q{[(1,2,3)]}, { pg_type => PG_CIRCLE }); };
like( $@, qr{Invalid input for geometric circle type}, 'DB handle method "quote" fails with invalid PG_CIRCLE string');
eval { $result = $dbh->quote(q{1,2,3,4,H}, { pg_type => PG_CIRCLE }); };
like( $@, qr{Invalid input for geometric circle type}, 'DB handle method "quote" fails with invalid PG_CIRCLE string');


#
# Test of the "quote_identifier" database handle method
#

%quotetests = (
									q{0} => q{"0"},
									q{Ain't misbehaving } => q{"Ain't misbehaving "},
									NULL => q{"NULL"},
									"" => q{""},
							);
for (keys %quotetests) {
	$result = $dbh->quote_identifier($_);
	is( $result, $quotetests{$_}, qq{DB handle method "quote_identifier" works with a value of "$_"});
}
is( $dbh->quote_identifier(undef), q{}, 'DB handle method "quote_identifier" works with an undefined value');

is ($dbh->quote_identifier( undef, 'Her schema', 'My table' ), q{"Her schema"."My table"}, 
		'DB handle method "quote_identifier" works with schemas');


#
# Test of the "table_attributes" database handle method (deprecated)
#

# Because this function is deprecated and really just calling the column_info() 
# and primary_key() methods, we will do minimal testing.
$result = $dbh->func('dbd_pg_test', 'table_attributes');
$result = $result->[0];
@required = 
	(qw(NAME TYPE SIZE NULLABLE DEFAULT CONSTRAINT PRIMARY_KEY REMARKS));
undef %missing;
for (@required) {
	$missing{$_}++ if ! exists $result->{$_};
}
is_deeply( \%missing, {}, 'DB handle method "table_attributes" returns the expected fields');

#
# Test of the "lo_*" database handle methods
#

$dbh->{AutoCommit}=1; $dbh->{AutoCommit}=0; ## Catch error where not in begin
my ($R,$W) = ($dbh->{pg_INV_READ}, $dbh->{pg_INV_WRITE});
my $RW = $R|$W;
my $object = $dbh->func($R, 'lo_creat');
like($object, qr/^\d+$/o, 'DB handle method "lo_creat" returns a valid descriptor for reading');
$object = $dbh->func($W, 'lo_creat');
like($object, qr/^\d+$/o, 'DB handle method "lo_creat" returns a valid descriptor for writing');

my $handle = $dbh->func($object, $W, 'lo_open');
like($handle, qr/^\d+$/o, 'DB handle method "lo_open" returns a valid descriptor for writing');

$result = $dbh->func($handle, 0, 0, 'lo_lseek');
cmp_ok($result, '==', 0, 'DB handle method "lo_lseek" works when writing');

my $buf = 'tangelo mulberry passionfruit raspberry plantain' x 500;
$result = $dbh->func($handle, $buf, length($buf), 'lo_write');
is( $result, length($buf), 'DB handle method "lo_write" works');

$result = $dbh->func($handle, 'lo_close');
ok( $result, 'DB handle method "lo_close" works after write');

# Reopen for reading
$handle = $dbh->func($object, $R, 'lo_open');
like($handle, qr/^\d+$/o, 'DB handle method "lo_open" returns a valid descriptor for reading');

$result = $dbh->func($handle, 11, 0, 'lo_lseek');
cmp_ok($result, '==', 11, 'DB handle method "lo_lseek" works when reading');

$result = $dbh->func($handle, 'lo_tell');
is( $result, 11, 'DB handle method "lo_tell" works');

$dbh->func($handle, 0, 0, 'lo_lseek');

my ($buf2,$data) = ('','');
while ($dbh->func($handle, $data, 513, 'lo_read')) {
	$buf2 .= $data;
}
is (length($buf), length($buf2), 'DB handle method "lo_read" read back the same data that was written');

$result = $dbh->func($handle, 'lo_close');
ok( $result, 'DB handle method "lo_close" works after read');

$result = $dbh->func($object, 'lo_unlink');
ok( $result, 'DB handle method "lo_unlink" works');

#
# Test of the "pg_notifies" database handle method
#

#  $ret = $dbh->func('pg_notifies');
# Returns either undef or a reference to two-element array [ $table,
# $backend_pid ] of asynchronous notifications received.

eval {
  $dbh->func('pg_notifies');
};
ok( !$@, 'DB handle method "pg_notifies" does not throw an error');

#
# Test of the "getfd" database handle method
#

$result = $dbh->func('getfd');
like( $result, qr/^\d+$/, 'DB handle method "getfd" returns a number');

#
# Test of the "state" database handle method
#

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
$result = $dbh->state();
is( $result, "", qq{DB handle method "state" returns an empty string on success});

eval {
	$dbh->do("SELECT dbdpg_throws_an_error");
};
$result = $dbh->state();
like( $result, qr/^[A-Z0-9]{5}$/, qq{DB handle method "state" returns a five-character code on error});
$dbh->rollback();

#
# Test of the "private_attribute_info" database handle method
#

SKIP: {
	if ($DBI::VERSION < 1.54) {
		skip "DBI must be at least version 1.54 to test private_attribute_info", 2;
	}

	my $private = $dbh->private_attribute_info();
	my ($valid,$invalid) = (0,0);
	for my $name (keys %$private) {
		$name =~ /^pg_\w+/ ? $valid++ : $invalid++;
	}
	ok($valid >= 1, qq{DB handle method "private_attribute_info" returns at least one record});
	is($invalid, 0, qq{DB handle method "private_attribute_info" returns only internal names});

}

#
# Test of the "ping" database handle method
#

ok( 1==$dbh->ping(), 'DB handle method "ping" returns 1 on an idle connection');

$dbh->do("SELECT 123");

$result = 3;
ok( $result==$dbh->ping(), 'DB handle method "ping" returns 3 for a good connection inside a transaction');

$dbh->commit();

ok( 1==$dbh->ping(), 'DB handle method "ping" returns 1 on an idle connection');

my $mtvar; ## This is an implicit test of getline: please leave this var undefined

$dbh->do("COPY dbd_pg_test(id,pname) TO STDOUT");
{
	local $SIG{__WARN__} = sub {};
	$dbh->pg_getline($mtvar,100);
	ok( 2==$dbh->ping(), 'DB handle method "ping" returns 2 when in COPY IN state');
	1 while $dbh->pg_getline($mtvar,1000);
	ok( 2==$dbh->ping(), 'DB handle method "ping" returns 2 immediately after COPY IN state');
}
	
$dbh->do("SELECT 123");
	
ok( 3==$dbh->ping(), 'DB handle method "ping" returns 3 for a good connection inside a transaction');
	
eval {
	$dbh->do("DBD::Pg creating an invalid command for testing");
};
ok( 4==$dbh->ping(), 'DB handle method "ping" returns a 4 when inside a failed transaction');

$dbh->disconnect();
ok( 0==$dbh->ping(), 'DB handle method "ping" fails (returns 0) on a disconnected handle');

$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											{RaiseError => 1, PrintError => 0, AutoCommit => 0});

ok( defined $dbh, "Reconnect to the database after disconnect");

#
# Test of the "pg_ping" database handle method
#

ok( 1==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 1 on an idle connection');

$dbh->do("SELECT 123");

ok( 3==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 3 for a good connection inside a transaction');

$dbh->commit();

ok( 1==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 1 on an idle connection');

$dbh->do("COPY dbd_pg_test(id,pname) TO STDOUT");
$dbh->pg_getline($mtvar,100);
ok( 2==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 2 when in COPY IN state');
1 while $dbh->pg_getline($mtvar,1000);
ok( 2==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 2 immediately after COPY IN state');

$dbh->do("SELECT 123");

ok( 3==$dbh->pg_ping(), 'DB handle method "pg_ping" returns 3 for a good connection inside a transaction');

eval {
	$dbh->do("DBD::Pg creating an invalid command for testing");
};
ok( 4==$dbh->pg_ping(), 'DB handle method "pg_ping" returns a 4 when inside a failed transaction');

$dbh->disconnect();
ok( -1==$dbh->pg_ping(), 'DB handle method "pg_ping" fails (returns 0) on a disconnected handle');

