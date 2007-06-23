#!perl -w

# Test all handle attributes: database, statement, and generic ("any")

use Test::More;
use DBI qw(:sql_types);
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 132;
}
else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 0, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, "Connect to database for handle attributes testing");

my $version = $dbh->{pg_server_version};
my $pglibversion = $dbh->{pg_lib_version};
my $got73 = $version >= 70300 ? 1 : 0;
if ($got73) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}

my $pgversion = $dbh->{pg_server_version};

my $attributes_tested = q{

d = database handle specific
s = statement handle specific
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
d pg_enable_utf8
d pg_db
d pg_user
d pg_pass
d pg_port
d pg_default_port
d pg_options
d pg_socket
d pg_pid

d pg_prepare_now - tested in 03smethod.t
d pg_server_prepare - tested in 03smethod.t
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

a Warn (inheritance test also)
a Active
a Kids
a ActiveKids
a CachedKids
a CompatMode
a PrintError
a RaiseError
a HandleError
a ShowErrorStatement (unsupported)
a TraceLevel
a FetchHashKeyName
a ChopBlanks
a LongReadLen
a LongTruncOk
a TaintIn
a TaintOut
a Taint
a Profile (not tested)

d InactiveDestroy (must be the last one tested)

};

my ($attrib,$SQL,$sth,$warning,$result);

#
# Test of the database handle attribute "Statement"
#

$SQL = "SELECT 123";
$sth = $dbh->prepare($SQL);
$sth->finish();

$attrib = $dbh->{Statement};
is( $attrib, $SQL, 'DB handle attribute "Statement" returns the last prepared query');

#
# Test of bogus database/statement handle attributes
#

## DBI switched from error to warnign in 1.43
$warning="";
eval {
	local $SIG{__WARN__} = sub { $warning = shift; };
	$dbh->{CrazyDiamond}=1;
};
ok( (length $warning or $@), 'Error or warning when setting an invalid database handle attribute');

eval {
	$dbh->{private_dbdpg_CrazyDiamond}=1;
};
ok( !$@, 'Setting a private attribute on a database handle does not throw an error');

$sth = $dbh->prepare('SELECT 123');

$warning="";
eval {
	local $SIG{__WARN__} = sub { $warning = shift; };
	$sth->{CrazyDiamond}=1;
};
ok( (length $warning or $@), 'Error or warning when setting an invalid statement handle attribute');

eval {
	$sth->{private_dbdpg_CrazyDiamond}=1;
};
ok( !$@, 'Setting a private attribute on a statement handle does not throw an error');

#
# Test of the database handle attribute "AutoCommit"
#

$dbh->do('DELETE FROM dbd_pg_test');
ok( $dbh->commit(), "Commit after deleting all rows from dbd_pg_test");

my $dbh2 = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
												{RaiseError => 0, PrintError => 0, AutoCommit => 1});
if ($got73) {
	$dbh2->do("SET search_path TO " . $dbh2->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}

ok( defined $dbh2, "Connect to database with second database handle, AutoCommit on");

ok( $dbh->do("INSERT INTO dbd_pg_test (id, pname, val) VALUES (1, 'Coconut', 'Mango')"),
		'Insert a row into the database with first database handle');


my $rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
cmp_ok($rows, '==', 0, 'Second database handle cannot see insert from first');

ok( $dbh->do("INSERT INTO dbd_pg_test (id, pname, val) VALUES (2, 'Grapefruit', 'Pomegranate')"),
		'Insert a row into the database with second database handle');

$rows = ($dbh->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 2}))[0];
cmp_ok($rows, '==', 1, 'First database handle can see insert from second');

ok( $dbh->commit, 'Commit transaction with first database handle');

$rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test WHERE id = 1}))[0];
cmp_ok($rows, '==', 1, 'Second database handle can see insert from first');

ok( $dbh2->disconnect(), 'Disconnect with second database handle');


#
# Test of the database handle attribute "Driver"
#

$attrib = $dbh->{Driver}->{Name};
is( $attrib, 'Pg', '$dbh->{Driver}{Name} returns correct value of "Pg"');

#
# Test of the database handle attribute "Name"
#

SKIP: {
	skip qq{Cannot test DB handle attribute "Name": no DBI_DSN is set}, 1
		if ! exists $ENV{DBI_DSN};
	skip qq{Cannot test DB handle attribute "Name": invalid DBI_DSN}, 1
		if $ENV{DBI_DSN} !~ /^dbi:Pg:(.+)$/io;

	$attrib = $dbh->{Name};
	is( $attrib, $1, 'DB handle attribute "Name" returns same value as DBI_DSN');
}

#
# Test of the database handle attribute "RowCacheSize"
#

$attrib = $dbh->{RowCacheSize};
ok( !defined $attrib, 'DB handle attribute "RowCacheSize" returns undef');
$dbh->{RowCacheSize} = 42;
$attrib = $dbh->{RowCacheSize};
ok( !defined $attrib, 'Setting DB handle attribute "RowCacheSize" has no effect');

#
# Test of the database handle attribute "Username"
#

SKIP: {
	skip 'DBI must be at least version 1.36 to test the DB handle attribute "Username"', 1
		if $DBI::VERSION < 1.36;

	$attrib = $dbh->{Username};
	is( $attrib, $ENV{DBI_USER}, 'DB handle attribute "Username" returns the same value as DBI_USER');
}

#
# Test of the "PrintWarn" database handle attribute
#

my $value = $dbh->{PrintWarn};
is ($value, 1, qq{DB handle attribute "PrintWarn" defaults to on});

{

local $SIG{__WARN__} = sub { $warning = shift; };

$warning = q{};
eval {
	$dbh->do("CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)");
};
ok (!$@, qq{DB handle attribute "PrintWarn" works when on});
like($warning, qr{dbd_pg_test_temp}, qq{DB handle attribute "PrintWarn" shows warnings when on});

$dbh->rollback();
$dbh->{PrintWarn}=0;
$warning = q{};
eval {
	$dbh->do("CREATE TEMP TABLE dbd_pg_test_temp(id INT PRIMARY KEY)");
};
ok (!$@, qq{DB handle attribute "PrintWarn" works when on});
is($warning, q{}, qq{DB handle attribute "PrintWarn" shows warnings when on});

$dbh->{PrintWarn}=1;
$dbh->rollback();

}


#
# Test of the database handle attributes "pg_INV_WRITE" and "pg_INV_READ"
# (these are used by the lo_* database handle methods)
#

like( $dbh->{pg_INV_WRITE}, qr/^\d+$/, 'Database handle attribute "pg_INV_WRITE" returns a number');
like( $dbh->{pg_INV_READ}, qr/^\d+$/, 'Database handle attribute "pg_INV_READ" returns a number');

#
# Test of the database handle attribute "pg_protocol"
#

like( $dbh->{pg_protocol}, qr/^\d+$/, 'Database handle attribute "pg_protocol" returns a number');

#
# Test of the database handle attribute "pg_errorlevel"
#

cmp_ok( 1, '==', $dbh->{pg_errorlevel}, 'Database handle attribute "pg_errorlevel" returns the default (1)');

SKIP: {
	skip 'Cannot test DB handle attribute "pg_errorlevel" on pre-7.4 servers', 1
		if $pgversion < 70400;

	$dbh->{pg_errorlevel} = 3;
	cmp_ok( 1, '==', $dbh->{pg_errorlevel}, 'Database handle attribute "pg_errorlevel" defaults to 1 if invalid');
}

#
# Test of the database handle attribute "pg_bool_tf"
#

$result = $dbh->{pg_bool_tf}=0;
is( $result, 0, 'DB handle method "pg_bool_tf" starts as 0');

$sth = $dbh->prepare("SELECT ?::bool");
$sth->bind_param(1,1,SQL_BOOLEAN);
$sth->execute();
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


## Test of all the informational pg_* database handle attributes

$result = $dbh->{pg_protocol};
like( $result, qr/^\d+$/, qq{DB handle attribute "pg_db" returns at least one character});

$result = $dbh->{pg_db};
ok( length $result, qq{DB handle attribute "pg_db" returns at least one character});

$result = $dbh->{pg_user};
ok( defined $result, qq{DB handle attribute "pg_user" returns a value});

$result = $dbh->{pg_pass};
ok( defined $result, qq{DB handle attribute "pg_pass" returns a value});

$result = $dbh->{pg_port};
like( $result, qr/^\d+$/, qq{DB handle attribute "pg_port" returns a number});

$result = $dbh->{pg_default_port};
like( $result, qr/^\d+$/, qq{DB handle attribute "pg_default_port" returns a number});

$result = $dbh->{pg_options};
ok (defined $result, qq{DB handle attribute "pg_options" returns a value});

$result = $dbh->{pg_socket};
like( $result, qr/^\d+$/, qq{DB handle attribute "pg_socket" returns a value});

$result = $dbh->{pg_pid};
like( $result, qr/^\d+$/, qq{DB handle attribute "pg_pid" returns a value});


# Attempt to test whether or not we can get unicode out of the database
SKIP: {
	eval "use Encode;";
	skip "Encode module is needed for unicode tests", 5 if $@;
	my $SQL = "SELECT id, pname FROM dbd_pg_test WHERE id = ?";
	my $sth = $dbh->prepare($SQL);
	$sth->execute(1);
	local $dbh->{pg_enable_utf8} = 1;
	my $utf8_str = chr(0x100).'dam';	# LATIN CAPITAL LETTER A WITH MACRON
	is( $dbh->quote( $utf8_str ), "'$utf8_str'", 'quote() handles utf8.' );
	$SQL = "INSERT INTO dbd_pg_test (id, pname, val) VALUES (40, '$utf8_str', 'Orange')";
	is( $dbh->do($SQL), '1', 'Able to insert unicode character into the database');
	$sth->execute(40);
	my ($id, $name) = $sth->fetchrow_array();
	ok( Encode::is_utf8($name), 'Able to read unicode (utf8) data from the database');
	is( length($name), 4, 'Unicode (utf8) data returned from database is not corrupted');
	$sth->finish();
	$sth->execute(1);
	my ($id2, $name2) = $sth->fetchrow_array();
	ok( !Encode::is_utf8($name2), 'ASCII text returned from database does not have utf8 bit set');
	$sth->finish();
}


#
# Use the handle attribute "Warn" to check inheritance
#

undef $sth;

ok( $dbh->{Warn}, 'Attribute "Warn" attribute set on by default');

$SQL = "SELECT 123";
$sth = $dbh->prepare($SQL);
$sth->finish();
ok( $sth->{Warn}, 'Statement handle inherits the "Warn" attribute');

$dbh->{Warn} = 0;
ok( ! $dbh->{Warn}, 'Turn off the "Warn" attribute in the database handle');

#
# Test of the the following statement handle attributes:
# NUM_OF_FIELDS, NUM_OF_PARAMS
# NAME, NAME_lc, NAME_uc, NAME_hash, NAME_lc_hash, NAME_uc_hash
# TYPE, PRECISION, SCALE, NULLABLE
#

$sth = $dbh->prepare('SELECT 123 AS "Sheep", CAST(id AS float) FROM dbd_pg_test WHERE id=?');
$sth->execute(12);
$attrib = $sth->{'NUM_OF_FIELDS'};
is( $attrib, '2', 'Statement handle attribute "NUM_OF_FIELDS" works correctly for SELECT');
$attrib = $sth->{'NUM_OF_PARAMS'};
is( $attrib, '1', 'Statement handle attribute "NUM_OF_PARAMS" works correctly with one placeholder');
$attrib = $sth->{NAME};
my $colnames = ['Sheep', 'id'];
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME" works correctly');
$attrib = $sth->{NAME_lc};
$colnames = ['sheep', 'id'];
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME_lc" works correctly');
$attrib = $sth->{NAME_uc};
$colnames = ['SHEEP', 'ID'];
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME_uc" works correctly');
$attrib = $sth->{'NAME_hash'};
$colnames = {'Sheep' => 0, id => 1};
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME_hash" works correctly');
$attrib = $sth->{'NAME_lc_hash'};
$colnames = {sheep => 0, id => 1};
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME_lc_hash" works correctly');
$attrib = $sth->{NAME_uc_hash};
$colnames = {SHEEP => 0, ID => 1};
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME_uc_hash" works correctly');

$attrib = $sth->{TYPE};
$colnames = [4, 6];
is_deeply( $attrib, $colnames, 'Statement handle attribute "TYPE" works correctly');

$attrib = $sth->{PRECISION};
$colnames = [4, 8];
is_deeply( $attrib, $colnames, 'Statement handle attribute "PRECISION" works correctly');

$attrib = $sth->{SCALE};
$colnames = [undef,undef];
is_deeply( $attrib, $colnames, 'Statement handle attribute "SCALE" works correctly');

$attrib = $sth->{NULLABLE};
$colnames = [2,2];
is_deeply( $attrib, $colnames, 'Statement handle attribute "NULLABLE" works correctly');

$sth->finish();

$sth = $dbh->prepare("DELETE FROM dbd_pg_test WHERE id=0");
$sth->execute();
$attrib = $sth->{'NUM_OF_FIELDS'};
my $expected = $DBI::VERSION >=1.42 ? undef : 0;
is( $attrib, $expected, 'Statement handle attribute "NUM_OF_FIELDS" works correctly for DELETE');
$attrib = $sth->{'NUM_OF_PARAMS'};
is( $attrib, '0', 'Statement handle attribute "NUM_OF_PARAMS" works correctly with no placeholder');
$attrib = $sth->{NAME};
$colnames = [];
is_deeply( $attrib, $colnames, 'Statement handle attribute "NAME" works correctly for DELETE');

$sth->finish();

#
# Test of the statement handle attribute "CursorName"
#

$attrib = $sth->{CursorName};
is( $attrib, undef, 'Statement handle attribute "CursorName" returns undef');

#
# Test of the statement handle attribute "Database"
#

$attrib = $sth->{Database};
is( $attrib, $dbh, 'Statement handle attribute "Database" matches the database handle');

#
# Test of the statement handle attribute "ParamValues"
#

$sth = $dbh->prepare("SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND pname=?");
$sth->bind_param(1, 99);
$sth->bind_param(3, 'Sparky');
$attrib = $sth->{ParamValues};
$expected = {1 => "99", 2 => undef, 3 => "Sparky"};
is_deeply( $attrib, $expected, qq{Statement handle attribute "ParamValues" works before execute});
$sth->execute();
$attrib = $sth->{ParamValues};
is_deeply( $attrib, $expected, qq{Statement handle attribute "ParamValues" works after execute});

#
# Test of the statement handle attribute "ParamTypes"
#

SKIP: {
	skip 'DBI must be at least version 1.49 to test the DB handle attribute "ParamTypes"', 2
		if $DBI::VERSION < 1.49;

	$sth = $dbh->prepare("SELECT id FROM dbd_pg_test WHERE id=? AND val=? AND lii=?");
	$sth->bind_param(1, 1, SQL_INTEGER);
	$sth->bind_param(2, 'TMW', SQL_VARCHAR);
	$attrib = $sth->{ParamTypes};
	$expected = {1 => "int4", 2 => "varchar", 3 => undef};
	is_deeply( $attrib, $expected, qq{Statement handle attribute "ParamTypes" works before execute});
	$sth->execute('TT');
	$attrib = $sth->{ParamTypes};
	is_deeply( $attrib, $expected, qq{Statement handle attribute "ParamTypes" works after execute});
}

#
# Test of the statement handle attribute "RowsInCache"
#

$attrib = $sth->{RowsInCache};
is( $attrib, undef, 'Statement handle attribute "RowsInCache" returns undef');


#
# Test of the statement handle attribute "pg_size"
#

$SQL = 'SELECT id, pname, val, score, Fixed, pdate, "CaseTest" FROM dbd_pg_test';
$sth = $dbh->prepare($SQL);
$sth->execute();
$result = $sth->{pg_size};
$expected = [qw(4 -1 -1 8 -1 8 1)];
is_deeply( $result, $expected, 'Statement handle attribute "pg_size" works');

#
# Test of the statement handle attribute "pg_size"
#

$sth->execute();
$result = $sth->{pg_type};
$expected = [qw(int4 varchar text float8 bpchar timestamp bool)];
# Hack for old servers
$expected->[5] = 'datetime' unless $got73;
is_deeply( $result, $expected, 'Statement handle attribute "pg_type" works');
$sth->finish();

#
# Test of the statement handle attribute "pg_oid_status"
#

$SQL = "INSERT INTO dbd_pg_test (id, val) VALUES (?, 'lemon')";
$sth = $dbh->prepare($SQL);
$sth->bind_param('$1','',SQL_INTEGER);
$sth->execute(500);
$result = $sth->{pg_oid_status};
like( $result, qr/^\d+$/, 'Statement handle attribute "pg_oid_status" returned a numeric value after insert');

#
# Test of the statement handle attribute "pg_cmd_status"
#

## INSERT DELETE UPDATE SELECT
for ("INSERT INTO dbd_pg_test (id,val) VALUES (400, 'lime')",
		 "DELETE FROM dbd_pg_test WHERE id=1",
		 "UPDATE dbd_pg_test SET id=2 WHERE id=2",
		 "SELECT * FROM dbd_pg_test"
		) {
	my $expected = substr($_,0,6);
	$sth = $dbh->prepare($_);
	$sth->execute();
	$result = $sth->{pg_cmd_status};
	$sth->finish();
	like ( $result, qr/^$expected/, qq{Statement handle attribute "pg_cmd_status" works for '$expected'});
}

#
# Test of the handle attribute "Active"
#

$attrib = $dbh->{Active};
is( $attrib, 1, 'Database handle attribute "Active" is true while connected');

$sth = $dbh->prepare("SELECT 123 UNION SELECT 456");
$attrib = $sth->{Active};
is($attrib, '', 'Statement handle attribute "Active" is false before SELECT');
$sth->execute();
$attrib = $sth->{Active};
is($attrib, 1, 'Statement handle attribute "Active" is true after SELECT');
my $row = $sth->fetchrow_arrayref();
$attrib = $sth->{Active};
is($attrib, 1, 'Statement handle attribute "Active" is true when rows remaining');
$sth->finish();
$attrib = $sth->{Active};
is($attrib, '', 'Statement handle attribute "Active" is false after finish called');

#
# Test of the handle attribute "Kids"
#

$attrib = $dbh->{Kids};
is( $attrib, 1, 'Database handle attribute "Kids" is set properly');
my $sth2 = $dbh->prepare("SELECT 234");
$attrib = $dbh->{Kids};
is( $attrib, 2, 'Database handle attribute "Kids" works');
$attrib = $sth2->{Kids};
is( $attrib, 0, 'Statement handle attribute "Kids" is zero');

#
# Test of the handle attribute "ActiveKids"
#

$attrib = $dbh->{ActiveKids};
is( $attrib, 0, 'Database handle attribute "ActiveKids" is set properly');
$sth2 = $dbh->prepare("SELECT 234");
$sth2->execute();
$attrib = $dbh->{ActiveKids};
is( $attrib, 1, 'Database handle attribute "ActiveKids" works');
$attrib = $sth2->{ActiveKids};
is( $attrib, 0, 'Statement handle attribute "ActiveKids" is zero');

#
# Test of the handle attribute "CachedKids"
#

$attrib = $dbh->{CachedKids};
ok( !$attrib, 'Database handle attribute "CachedKids" is set properly');

#
# Test of the handle attribute "CompatMode"
#

$attrib = $dbh->{CompatMode};
ok( !$attrib, 'Database handle attribute "CompatMode" is set properly');

#
# Test of the handle attribute PrintError
#

$attrib = $dbh->{PrintError};
is( $attrib, '', 'Database handle attribute "PrintError" is set properly');


# Make sure that warnings are sent back to the client
# We assume that older servers are okay
my $client_level = '';
if ($pgversion >= 70300) {
	$sth = $dbh->prepare("SHOW client_min_messages");
	$sth->execute();
	$client_level = $sth->fetchall_arrayref()->[0][0];
}

if ($client_level eq "error") {
 SKIP: {
		skip qq{Cannot test "PrintError" attribute because client_min_messages is set to 'error'}, 2;
	}
 SKIP: {
		skip qq{Cannot test "RaiseError" attribute because client_min_messages is set to 'error'}, 2;
	}
 SKIP: {
		skip qq{Cannot test "HandleError" attribute because client_min_messages is set to 'error'}, 2;
	}
}
else {
	$SQL = "Testing the DBD::Pg modules error handling -?-";
	{
		$warning = '';
		local $SIG{__WARN__} = sub { $warning = shift; };
		$dbh->{RaiseError} = 0;
		
		$dbh->{PrintError} = 1;
		$sth = $dbh->prepare($SQL);
		$sth->execute();
		ok( $warning, 'Warning thrown when database handle attribute "PrintError" is on');

		undef $warning;
		$dbh->{PrintError} = 0;
		$sth = $dbh->prepare($SQL);
		$sth->execute();
		ok( !$warning, 'No warning thrown when database handle attribute "PrintError" is off');
	}
}

#
# Test of the handle attribute RaiseError
#

if ($client_level ne "error") {
	$dbh->{RaiseError} = 0;
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	ok (!$@, 'No error produced when database handle attribute "RaiseError" is off');
	
	$dbh->{RaiseError} = 1;
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	ok ($@, 'Error produced when database handle attribute "RaiseError" is off');
}


#
# Test of the handle attribute HandleError
#

$attrib = $dbh->{HandleError};
ok( !$attrib, 'Database handle attribute "HandleError" is set properly');

if ($client_level ne "error") {

	undef $warning;
	$dbh->{HandleError} = sub { $warning = shift; };
	$sth = $dbh->prepare($SQL);
	$sth->execute();
	ok( $warning, 'Database handle attribute "HandleError" works');
	# Test changing values
	undef $warning;
	$dbh->{HandleError} = sub { $_[0] = "Slonik $_[0]"; 0; };
	eval {
		$sth = $dbh->prepare($SQL);
		$sth->execute();
	};
	like($@, qr/^Slonik/, 'Database handle attribute "HandleError" modifies error messages');
	$dbh->{HandleError}= undef;
	$dbh->rollback();
}


#
# Not supported yet: ShowErrorStatement
#

#
# Test of the handle attribute TraceLevel
#

$attrib = $dbh->{TraceLevel};
like($attrib, qr/^\d$/, qq{Database handle attribute "TraceLevel" returns a number ($attrib)});

#
# Test of the handle attribute FetchHashKeyName
#

# The default is mixed case ("NAME");
$attrib = $dbh->{FetchHashKeyName};
is( $attrib, 'NAME', 'Database handle attribute "FetchHashKeyName" is set properly');

$SQL = qq{SELECT "CaseTest" FROM dbd_pg_test};
$sth = $dbh->prepare($SQL);
$sth->execute();
my ($colname) = keys %{$sth->fetchrow_hashref()};
is( $colname, 'CaseTest', 'Database handle attribute "FetchHashKeyName" works with the default value of NAME');
$sth->finish();

$dbh->{FetchHashKeyName} = "NAME_lc";
$attrib = $dbh->{FetchHashKeyName};
is( $attrib, 'NAME_lc', 'Database handle attribute "FetchHashKeyName" can be changed');

$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
is( $colname, 'casetest', 'Database handle attribute "FetchHashKeyName" works with a value of NAME_lc');
$sth->finish();

$dbh->{FetchHashKeyName} = "NAME_uc";
$sth = $dbh->prepare($SQL);
$sth->execute();
($colname) = keys %{$sth->fetchrow_hashref()};
is( $colname, 'CASETEST', 'Database handle attribute "FetchHashKeyName" works with a value of NAME_uc');
$sth->finish();
$dbh->{FetchHashKeyName} = "NAME";

#
# Test of the handle attribute ChopBlanks
#


$attrib = $dbh->{ChopBlanks};
ok( !$attrib, 'Database handle attribute "ChopBlanks" is set properly');

$dbh->do("DELETE FROM dbd_pg_test");
$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed, val) VALUES (3, ' Fig', ' Raspberry ')});

$dbh->{ChopBlanks} = 0;
my ($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is( $val, ' Fig ', 'Database handle attribute "ChopBlanks" = 0 returns correct value for fixed-length column');
($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
is( $val, ' Raspberry ', 'Database handle attribute "ChopBlanks" = 0 returns correct value for variable-length column');

$dbh->{ChopBlanks}=1;

($val) = $dbh->selectall_arrayref(q{SELECT fixed FROM dbd_pg_test WHERE id = 3})->[0][0];
is( $val, ' Fig', 'Database handle attribute "ChopBlanks" = 1 returns correct value for fixed-length column');

($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 3});
is( $val, ' Raspberry ', 'Database handle attribute "ChopBlanks" = 1 returns correct value for variable-length column');
$dbh->do("DELETE from dbd_pg_test");

#
# Test of the handle attribute LongReadLen
#

$attrib = $dbh->{LongReadLen};
ok( $attrib, 'Handle attribute "LongReadLen" has been set properly');

#
# Test of the handle attribute LongTruncOk
#

$attrib = $dbh->{LongTruncOk};
ok( !$attrib, 'Handle attribute "LongTruncOk" has been set properly');

#
# Test of the handle attribute TaintIn
#

$attrib = $dbh->{TaintIn};
is( $attrib, '', 'Handle attribute "TaintIn" has been set properly');

#
# Test of the handle attribute TaintOut
#

$attrib = $dbh->{TaintOut};
is( $attrib, '', 'Handle attribute "TaintOut" has been set properly');

#
# Test of the handle attribute Taint
#
$attrib = $dbh->{Taint};
is( $attrib, '', 'Handle attribute "Taint" has been set properly');

$dbh->{Taint}=1;

$attrib = $dbh->{Taint};
is( $attrib, 1, 'The value of handle attribute "Taint" can be changed');
$attrib = $dbh->{TaintIn};
is( $attrib, 1, 'Changing handle attribute "Taint" changes "TaintIn"');
$attrib = $dbh->{TaintOut};
is( $attrib, 1, 'Changing handle attribute "Taint" changes "TaintOut"');

#
# Not tested: handle attribute Profile
#

#
# Test of the database handle attribute InactiveDestroy
# This one must be the last test performed!
#

$attrib = $dbh->{InactiveDestroy};
ok( !$attrib, 'Database handle attribute "InactiveDestroy" is set properly');

# Disconnect in preparation for the fork tests
ok( $dbh->disconnect(), 'Disconnect from database');
$attrib = $dbh->{Active};
is( $attrib, '', 'Database handle attribute "Active" is false after disconnect');

SKIP: {
	skip 'Cannot test database handle "InactiveDestroy" on a non-forking system', 4
		if $^O =~ /MSWin/;

	require Test::Simple;

	SKIP: {
		skip 'Test::Simple version 0.47 or better required for testing of attribute "InactiveDestroy"', 4
			if $Test::Simple::VERSION < 0.47;

		# Test of forking. Hang on to your hats

		my $answer = 42;
		$SQL = "SELECT $answer FROM dbd_pg_test WHERE id > ? LIMIT 1";

		for my $destroy (0,1) {

			$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
													{RaiseError => 0, PrintError => 0, AutoCommit => 1});

			$sth = $dbh->prepare($SQL);
			$sth->execute(1);
			$sth->finish();

			# Desired flow: parent test, child test, child kill, parent test

			if (fork) {
				$sth->execute(1);
				my $val = $sth->fetchall_arrayref()->[0][0];
				is( $val, $answer, qq{Parent in fork test is working properly ("InactiveDestroy" = $destroy)});
				# Let the child exit
				select(undef,undef,undef,0.3);
			}
			else { # Child
				$dbh->{InactiveDestroy} = $destroy;
				select(undef,undef,undef,0.1); # Age before beauty
				exit; ## Calls disconnect via DESTROY unless InactiveDestroy set
			}

			if ($destroy) {
				# The database handle should still be active
				ok ( $dbh->ping(), qq{Ping works after the child has exited ("InactiveDestroy" = $destroy)});
				my $state = $dbh->state();
				is( $state, '', qq{Successful ping returns a SQLSTATE code of 00000 (empty string)});
				## The statement handle should still be usable
				$sth->execute(1);
				my $val = $sth->fetchall_arrayref()->[0][0];
				is ($val, $answer, qq{Statement handle works after forking});
			}
			else {
				# The database handle should be dead
				ok ( !$dbh->ping(), qq{Ping fails after the child has exited ("InactiveDestroy" = $destroy)});
				my $state = $dbh->state();
				is( $state, 'S8006', qq{Failed ping returns a SQLSTATE code of S8006});
				SKIP: {
					skip "Can't determine advanced ping with old 7.2 server libraries", 1
						if $pglibversion < 70400;

					ok ( -2==$dbh->pg_ping(), qq{pg_ping gives an error code of -2 after the child has exited ("InactiveDestroy" = $destroy)});
				}
			}
		}
	}
}
