#!perl -w

# Test of the database handle methods
# The following methods are *not* currently tested here:
# "clone"
# "data_sources"
# "do"
# "prepare"
# "disconnect"
# "prepare_cached"
# "take_imp_data"
# "lo_import"
# "lo_export"

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 139;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for database handle method testing");

my ($SQL, $sth, $result, @result, $expected, $warning, $rows);

# Populate the testing table for later use

$dbh->do("DELETE FROM dbd_pg_test");
$SQL = "INSERT INTO dbd_pg_test(id,val) VALUES (?,?)";
$sth = $dbh->prepare($SQL);
$sth->execute(10,'Roseapple');
$sth->execute(11,'Pineapple');
$sth->execute(12,'Kiwi');

#
# Test of the "last_insert_id" database handle method
#

TODO: {
	local $TODO = 'DB handle method "last_insert_id" is not implemented yet';
}

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

if ($DBI::VERSION < 1.36) {
 SKIP: {
		skip 'DBI must be at least version 1.36 to test "selectcol_arrayref" with "MaxRows"', 1;
	}
}
else {
	$result = $dbh->selectcol_arrayref($SQL, {Columns=>[2], MaxRows => 1});
	$expected = ['Roseapple'];
	is_deeply($result, $expected, 'DB handle method "selectcol_arrayref" works with the MaxRows attribute');
}

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
$dbh->{AutoCommit}=0;

#
# Test of the "get_info" database handle method
#

eval {
  $dbh->get_info();
};
ok ($@, 'DB handle method "get_info" with no arguments gives an error');

my %get_info = (
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
is( $result->{pg_type_only}, 'integer', qq{DB handle method "column_info" returns good value for 'pg_type_only'});

## Check some of the returned fields:
is( $result->{TABLE_CAT}, undef, 'DB handle method "column_info" returns proper TABLE_CAT');
is( $result->{TABLE_NAME}, 'dbd_pg_test', 'DB handle method "column_info returns proper TABLE_NAME');
is( $result->{COLUMN_NAME}, 'id', 'DB handle method "column_info" returns proper COLUMN_NAME');
is( $result->{DATA_TYPE}, 4, 'DB handle method "column_info" returns proper DATA_TYPE');
is( $result->{COLUMN_SIZE}, 4, 'DB handle method "column_info" returns proper COLUMN_SIZE');
is( $result->{NULLABLE}, '0', 'DB handle method "column_info" returns proper NULLABLE');
is( $result->{REMARKS}, undef, 'DB handle method "column_info" returns proper REMARKS');
is( $result->{COLUMN_DEF}, undef, 'DB handle method "column_info" returns proper COLUMN_DEF');
is( $result->{ORDINAL_POSITION}, 1, 'DB handle method "column_info" returns proper ORDINAL_POSITION');
is( $result->{IS_NULLABLE}, 'NO', 'DB handle method "column_info" returns proper IS_NULLABLE');
is( $result->{pg_type}, 'integer', 'DB handle method "column_info" returns proper pg_type');
is( $result->{pg_type_only}, 'integer', 'DB handle method "column_info" returns proper pg_type_only');

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

#
# Test of the "foreign_key_info" database handle method
#

## Neither pktable nor fktable specified
$sth = $dbh->foreign_key_info(undef,undef,undef,undef,undef,undef);
is ($sth, undef, 'DB handle method "foreign_key_info" returns undef: no pk / no fk');

## All foreign_key_info tests are meaningless for old servers
my $pgversion = DBD::Pg::_pg_server_version($dbh);
if (! DBD::Pg::_pg_check_version(7.3, $pgversion)) {
 SKIP: {
		skip qq{Cannot test DB handle method "foreign_key_info" on pre-7.3 servers.}, 16;
	}
}
else {

# Since the schema name is part of our test, let's encourage 'public':
$dbh->do("SET search_path TO 'public'");

# Drop any tables that may exist
$SQL = "SELECT relname FROM pg_catalog.pg_class WHERE relkind='r' AND relname LIKE 'dbd_pg_test_'";
for (@{$dbh->selectall_arrayref($SQL)}) {
	$dbh->do("DROP TABLE $_->[0] CASCADE");
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
					 undef,
					 'public',
					 'dbd_pg_test1',
					 'a',
					 undef,
					 'public',
					 'dbd_pg_test2',
					 'f2',
					 2,
					 3,
					 3,
					 'dbd_pg_test2_fk1',
					 'dbd_pg_test1_pk',
					 '7',
					 'PRIMARY',
					 'int4',
					 'int4'
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
					 'public',
					 'dbd_pg_test1',
					 'b',
					 undef,
					 'public',
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
					 'public',
					 'dbd_pg_test1',
					 'c',
					 undef,
					 'public',
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
					 'public',
					 'dbd_pg_test1',
					 'a',
					 undef,
					 'public',
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
					 'public',
					 'dbd_pg_test1',
					 'c',
					 undef,
					 'public',
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
$dbh->do("DROP TABLE dbd_pg_test3");
$dbh->do("DROP TABLE dbd_pg_test2");
$dbh->do("DROP TABLE dbd_pg_test1");

# Reset the search path by reconnecting
$dbh->disconnect();

$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
										{RaiseError => 1, PrintError => 0, AutoCommit => 0});

} # end giant foreign_key_info bypass

#
# Test of the "tables" database handle method
#

@result = $dbh->tables('', '', 'dbd_pg_test', '');
like( $result[0], qr/dbd_pg_test/, 'DB handle method "tables" works');

@result = $dbh->tables('', '', 'dbd_pg_test', '', {noprefix => 1});
is( $result[0], 'dbd_pg_test', 'DB handle method "tables" works with a "noprefix" attribute');

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
is( $dbh->quote(undef), q{NULL}, 'DB handle method "quote" works with an undefined value');
is( $dbh->quote(1, 4), 1, 'DB handle method "quote" works with a supplied data type argument');

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

$dbh->{AutoCommit}=0;
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
# Test of the "putline" database handle method
#

# Older servers cannot handle column names in COPY
if (! DBD::Pg::_pg_check_version(7.3, $pgversion)) {
 SKIP: {
		skip qq{Cannot test DB handle method "putline" on pre-7.3 servers.}, 2;
	}
}
else {

$dbh->do("COPY dbd_pg_test (id, val) FROM STDIN");
$result = $dbh->func("13\tOlive\n", 'putline');
$result = $dbh->func("14\tStrawberry\n", 'putline');
$result = $dbh->func("15\tBlueberry\n", 'putline');
is( $result, 1,'DB handle method "putline" works when inserting a line');
$dbh->func("\\.\n", 'putline');
$dbh->func('endcopy');
$dbh->commit();

$expected = [[13 => 'Olive'],[14 => 'Strawberry'],[15 => 'Blueberry']];
$result = $dbh->selectall_arrayref("SELECT id,val FROM dbd_pg_test WHERE id BETWEEN 13 AND 15 ORDER BY id ASC");
is_deeply( $result, $expected, 'DB handle method "putline" copies strings to the database');

}


#
# Test of the "getline" database handle method
#

if (! DBD::Pg::_pg_check_version(7.3, $pgversion)) {
 SKIP: {
		skip qq{Cannot test DB handle method "getline" on pre-7.3 servers.}, 4;
	}
}
else {

$dbh->do("COPY dbd_pg_test (id, val) TO STDOUT");
my ($buffer,$badret,$badval) = ('',0,0);
while ($result = $dbh->func($buffer, 100, 'getline')) {
	$badret++ if $result !=1;
	$badval++ if $buffer !~ /^\d+\t\w+$/o;
}
$dbh->func('endcopy');
$dbh->commit();
is( $result, '', 'DB handle method "getline" returns empty string when finished');
is( $buffer, '\.', qq{DB handle method "getline" returns '\\.' when finished});
ok( !$badret, 'DB handle method "getline" returns a 1 for each row fetched');
ok( !$badval, 'DB handle method "getline" properly retrieved every row');

}

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
# Test of the "pg_bool_tf" database handle method
#

$result = $dbh->{pg_bool_tf}=0;
is( $result, 0, 'DB handle method "pg_bool_tf" starts as 0');

$sth = $dbh->prepare("SELECT ?::bool");
$sth->execute(1);
$result = $sth->fetchall_arrayref()->[0][0];
is( $result, "1", qq{DB handle method "pg_bool_tf" returns '1' for true when on});
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is( $result, "0", qq{DB handle method "pg_bool_tf" returns '0' for false when on});

$dbh->{pg_bool_tf}=1;
$sth->execute(1);
$result = $sth->fetchall_arrayref()->[0][0];
is( $result, 't', qq{DB handle method "pg_bool_tf" returns 't' for true when on});
$sth->execute(0);
$result = $sth->fetchall_arrayref()->[0][0];
is( $result, 'f', qq{DB handle method "pg_bool_tf" returns 'f' for true when on});

#
# Test of the "ping" database handle method
# This one must be the last test performed!
#

my $dbh2;
$result = $dbh->ping();
ok( $dbh->ping(), 'DB handle method "ping" works on an active connection');
$dbh->disconnect();
ok( ! $dbh->ping(), 'DB handle method "ping" fails on a disconnected handle');

