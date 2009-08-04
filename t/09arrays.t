#!perl

## Test arrays

use 5.006;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
use DBI     ':sql_types';
use DBD::Pg ':pg_types';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 257;

isnt ($dbh, undef, 'Connect to database for array testing');

my ($sth,$result,$t);

my $pgversion = $dbh->{pg_server_version};

if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

my $SQL = q{DELETE FROM dbd_pg_test WHERE pname = 'Array Testing'};
my $cleararray = $dbh->prepare($SQL);

$SQL = q{INSERT INTO dbd_pg_test(id,pname,testarray) VALUES (99,'Array Testing',?)};
my $addarray = $dbh->prepare($SQL);

$SQL = q{INSERT INTO dbd_pg_test(id,pname,testarray2) VALUES (99,'Array Testing',?)};
my $addarray_int = $dbh->prepare($SQL);

$SQL = q{INSERT INTO dbd_pg_test(id,pname,testarray3) VALUES (99,'Array Testing',?)};
my $addarray_bool = $dbh->prepare($SQL);

$SQL = q{SELECT testarray FROM dbd_pg_test WHERE pname= 'Array Testing'};
my $getarray = $dbh->prepare($SQL);

$SQL = q{SELECT testarray2 FROM dbd_pg_test WHERE pname= 'Array Testing'};
my $getarray_int = $dbh->prepare($SQL);

$SQL = q{SELECT testarray3 FROM dbd_pg_test WHERE pname= 'Array Testing'};
my $getarray_bool = $dbh->prepare($SQL);

$t='Array quoting allows direct insertion into statements';
$SQL = q{INSERT INTO dbd_pg_test (id,testarray2) VALUES };
my $quoteid = $dbh->quote(123);
my $quotearr = $dbh->quote([456]);
$SQL .= qq{($quoteid, $quotearr)};
eval {
	$dbh->do($SQL);
};
is ($@, q{}, $t);
$dbh->rollback();

## Input
## Expected
## Name of test

my $array_tests =
q!['']
{""}
Empty array

[['']]
{{""}}
Empty array with two levels

[[['']]]
{{{""}}}
Empty array with three levels

[[''],['']]
{{""},{""}}
Two empty arrays

[[[''],[''],['']]]
{{{""},{""},{""}}}
Three empty arrays at second level

[[],[[]]]
ERROR: must be of equal size
Unbalanced empty arrays

{}
ERROR: Cannot bind a reference
Bare hashref

[{}]
ERROR: only scalars and other arrays
Hashref at top level

[1,2,{3,4},5]
ERROR: only scalars and other arrays
Hidden hashref

[[1,2],[3]]
ERROR: must be of equal size
Unbalanced array

[[1,2],[3,4,5]]
ERROR: must be of equal size
Unbalanced array

[[1,2],[]]
ERROR: must be of equal size
Unbalanced array

[[],[3]]
ERROR: must be of equal size
Unbalanced array

[123]
{123} quote: {"123"}
Simple 1-D numeric array

['abc']
{abc} quote: {"abc"}
Simple 1-D text array

['a','b,c']
{a,"b,c"} quote: {"a","b,c"}
Text array with commas and quotes

['a','b,}']
{a,"b,}"} quote: {"a","b,}"}
Text array with commas, escaped closing brace

['a','b,]']
{a,"b,]"} quote: {"a","b,]"}
Text array with commas, escaped closing bracket

[1,2]
{1,2} quote: {"1","2"}
Simple 1-D numeric array

[[1]]
{{1}} quote: {{"1"}}
Simple 2-D numeric array

[[1,2]]
{{1,2}} quote: {{"1","2"}}
Simple 2-D numeric array

[[[1]]]
{{{1}}} quote: {{{"1"}}}
Simple 3-D numeric array

[[["alpha",2],[23,"pop"]]]
{{{alpha,2},{23,pop}}} quote: {{{"alpha","2"},{"23","pop"}}}
3-D mixed array

[[[1,2,3],[4,5,"6"],["seven","8","9"]]]
{{{1,2,3},{4,5,6},{seven,8,9}}} quote: {{{"1","2","3"},{"4","5","6"},{"seven","8","9"}}}
3-D mixed array

[q{O'RLY?}]
{O'RLY?} quote: {"O'RLY?"}
Simple single quote

[q{O"RLY?}]
{"O\"RLY?"}
Simple double quote

[[q{O"RLY?}],[q|'Ya' - "really"|],[123]]
{{"O\"RLY?"},{"'Ya' - \"really\""},{123}} quote: {{"O\"RLY?"},{"'Ya' - \"really\""},{"123"}}
Many quotes

["Single\\\\Backslash"]
{"Single\\\\\\\\Backslash"} quote: {"Single\\\\\\\\Backslash"}
Single backslash testing

["Double\\\\\\\\Backslash"]
{"Double\\\\\\\\\\\\\\\\Backslash"} quote: {"Double\\\\\\\\\\\\\\\\Backslash"}
Double backslash testing

[["Test\\\nRun","Quite \"so\""],["back\\\\\\\\slashes are a \"pa\\\\in\"",123] ]
{{"Test\\\\\\\\nRun","Quite \"so\""},{"back\\\\\\\\\\\\\\\\slashes are a \"pa\\\\\\\\in\"",123}} quote: {{"Test\\\\\\\nRun","Quite \"so\""},{"back\\\\\\\\\\\\\\\\slashes are a \"pa\\\\\\\\in\"","123"}}
Escape party - backslash+newline, two + one

[undef]
{NULL}
NEED 80200: Simple undef test

[[undef]]
{{NULL}}
NEED 80200: Simple undef test

[[1,2],[undef,3],["four",undef],[undef,undef]]
{{1,2},{NULL,3},{four,NULL},{NULL,NULL}} quote: {{"1","2"},{NULL,"3"},{"four",NULL},{NULL,NULL}}
NEED 80200: Multiple undef test

!;

## Note: We silently allow things like this: [[[]],[]]

$dbh->{pg_expand_array} = 0;

for my $test (split /\n\n/ => $array_tests) {
	next unless $test =~ /\w/;
	my ($input,$expected,$msg) = split /\n/ => $test;
	my $qexpected = $expected;
	if ($expected =~ s/\s*quote:\s*(.+)//) {
		$qexpected = $1;
	}
	if ($qexpected !~ /^ERROR/) {
		$qexpected = qq{'$qexpected'};
	}

	if ($msg =~ s/NEED (\d+):\s*//) {
		my $ver = $1;
		if ($pgversion < $ver) {
		  SKIP: {
				skip ('Cannot test NULL arrays unless version 8.2 or better', 4);
			}
			next;
		}
	}

	$t="Correct array inserted: $msg : $input";
	$cleararray->execute();
	eval {
		$addarray->execute(eval $input);
	};
	if ($expected =~ /error:\s+(.+)/i) {
		like ($@, qr{$1}, "Array failed : $msg : $input");
		like ($@, qr{$1}, "Array failed : $msg : $input");
	}
	else {
		is ($@, q{}, "Array worked : $msg : $input");
		$getarray->execute();
		$result = $getarray->fetchall_arrayref()->[0][0];
		is ($result, $expected, $t);
	}

	$t="Array quote worked : $msg : $input";
	eval {
		$result = $dbh->quote(eval $input );
	};
	if ($qexpected =~ /error:\s+(.+)/i) {
		my $errmsg = $1;
		$errmsg =~ s/bind/quote/;
		like ($@, qr{$errmsg}, "Array quote failed : $msg : $input");
		like ($@, qr{$errmsg}, "Array quote failed : $msg : $input");
	}
	else {
		is ($@, q{}, $t);

		$t="Correct array quote: $msg : $input";
		is ($result, $qexpected, $t);
	}

}


## Same thing, but expand the arrays
$dbh->{pg_expand_array} = 1;

for my $test (split /\n\n/ => $array_tests) {
	next unless $test =~ /\w/;
	my ($input,$expected,$msg) = split /\n/ => $test;
	my $qexpected = $expected;
	if ($expected =~ s/\s*quote:\s*(.+)//) {
		$qexpected = $1;
	}

	if ($msg =~ s/NEED (\d+):\s*//) {
		my $ver = $1;
		if ($pgversion < $ver) {
		  SKIP: {
				skip ('Cannot test NULL arrays unless version 8.2 or better', 2);
			}
			next;
		}
	}

	$t="Array worked : $msg : $input";
	$cleararray->execute();
	eval {
		$addarray->execute(eval $input);
	};
	if ($expected =~ /error:\s+(.+)/i) {
		like ($@, qr{$1}, "Array failed : $msg : $input");
		like ($@, qr{$1}, "Array failed : $msg : $input");
	}
	else {
		is ($@, q{}, $t);

		$t="Correct array inserted: $msg : $input";
		$getarray->execute();
		$result = $getarray->fetchall_arrayref()->[0][0];
		$qexpected =~ s/{}/{''}/;
		$qexpected =~ y/{}/[]/;
		$qexpected =~ s/NULL/undef/g;
		if ($msg =~ /closing brace/) {
			$qexpected =~ s/]"/}"/;
		}
		$expected = eval $qexpected;
		is_deeply ($result, $expected, $t);
	}

	if ($msg =~ /STOP/) {
		warn "Exiting for DEBUGGING. Result is:\n";
		warn Dumper $result;
		cleanup_database($dbh,'test');
		$dbh->disconnect;
		exit;
	}

}


## Test of no-item and empty string arrays

$t=q{String array with no items returns empty array};
$cleararray->execute();
$addarray->execute('{}');
$getarray->execute();
$result = $getarray->fetchall_arrayref();
is_deeply ($result, [[[]]], $t);

$t=q{String array with empty string returns empty string};
$cleararray->execute();
$addarray->execute('{""}');
$getarray->execute();
$result = $getarray->fetchall_arrayref();
is_deeply ($result, [[['']]], $t);

## Test non-string array variants

$t=q{Integer array with no items returns empty array};
$cleararray->execute();
$addarray_int->execute('{}');
$getarray_int->execute();
$result = $getarray_int->fetchall_arrayref();
is_deeply ($result, [[[]]], $t);

$t=q{Boolean array with no items returns empty array};
$cleararray->execute();
$addarray_bool->execute('{}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[]]], $t);

$t=q{Boolean array gets created and returned correctly};
$cleararray->execute();
$addarray_bool->execute('{1}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[1]]], $t);

$cleararray->execute();
$addarray_bool->execute('{0}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[0]]], $t);

$cleararray->execute();
$addarray_bool->execute('{t}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[1]]], $t);

$cleararray->execute();
$addarray_bool->execute('{f}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[0]]], $t);

$cleararray->execute();
$addarray_bool->execute('{f,t,f,0,1,1}');
$getarray_bool->execute();
$result = $getarray_bool->fetchall_arrayref();
is_deeply ($result, [[[0,1,0,0,1,1]]], $t);

## Pure string to array conversion testing

my $array_tests_out =
q!1
[1]
Simple test of single array element

1,2
[1,2]
Simple test of multiple array elements

1,2,3
[1,2,3]
Simple test of multiple array elements

'a','b'
['a','b']
Array with text items

0.1,2.4
[0.1,2.4]
Array with numeric items

'My"lrd','b','c'
['My"lrd','b','c']
Array with escaped items

[1]
[[1]]
Multi-level integer array

[[1,2]]
[[[1,2]]]
Multi-level integer array

[[1],[2]]
[[[1],[2]]]
Multi-level integer array

[[1],[2],[3]]
[[[1],[2],[3]]]
Multi-level integer array

[[[1]],[[2]],[[3]]]
[[[[1]],[[2]],[[3]]]]
Multi-level integer array

'abc',NULL
['abc',undef]
NEED 80200: Array with a null

['abc','NULL',NULL,NULL,123::text]
[['abc','NULL',undef,undef,'123']]
NEED 80200: Array with many nulls and a quoted int

['abc','']
[['abc','']]
Final item is empty

1,NULL
[1,undef]
NEED 80200: Last item is NULL

NULL
[undef]
NEED 80200: Only item is NULL

NULL,NULL
[undef,undef]
NEED 80200: Two NULL items only

NULL,NULL,NULL
[undef,undef,undef]
NEED 80200: Three NULL items only

[123,NULL,456]
[[123,undef,456]]
NEED 80200: Middle item is NULL

NULL,'abc'
[undef,'abc']
NEED 80200: First item is NULL

'a','NULL'
['a',"NULL"]
Fake NULL is text

[[[[[1,2,3]]]]]
[[[[[[1,2,3]]]]]]
Deep nesting

[[[[[1],[2],[3]]]]]
[[[[[[1],[2],[3]]]]]]
Deep nesting

[[[[[1]]],[[[2]]],[[[3]]]]]
[[[[[[1]]],[[[2]]],[[[3]]]]]]
Deep nesting

[[[[[1]],[[2]],[[3]]]]]
[[[[[[1]],[[2]],[[3]]]]]]
Deep nesting

1::bool
[1]
Test of boolean type

1::bool,0::bool,'true'::boolean
[1,0,1]
Test of boolean types

1::oid
[1]
Test of oid type - should not quote

1::text
['1']
Text number should quote

1,2,3
[1,2,3]
Unspecified int should not quote

1::int
[1]
Integer number should quote

'(1,2),(4,5)'::box,'(5,3),(4,5)'
['(4,5),(1,2)','(5,5),(4,3)']
Type 'box' works

!;

$Data::Dumper::Indent = 0;

for my $test (split /\n\n/ => $array_tests_out) {
	next unless $test =~ /\w/;
	my ($input,$expected,$msg) = split /\n/ => $test;
	my $qexpected = $expected;
	if ($expected =~ s/\s*quote:\s*(.+)//) {
		$qexpected = $1;
	}
	if ($msg =~ s/NEED (\d+):\s*//) {
		my $ver = $1;
		if ($pgversion < $ver) {
		  SKIP: {
				skip ('Cannot test NULL arrays unless version 8.2 or better', 1);
			}
			next;
		}
	}
	if ($pgversion < 80200) {
		if ($input =~ /SKIP/ or $test =~ /Fake NULL|boolean/) {
		  SKIP: {
				skip ('Cannot test some array items on pre-8.2 servers', 1);
			}
			next;
		}
	}

	$t="Array test $msg : $input";
	$SQL = qq{SELECT ARRAY[$input]};
	$result = '';
	eval {
		$result = $dbh->selectall_arrayref($SQL)->[0][0];
	};
	if ($result =~ /error:\s+(.+)/i) {
		like ($@, qr{$1}, "Array failed : $msg : $input");
	}
	else {
		$expected = eval $expected;
		## is_deeply does not handle type differences
		is ( (Dumper $result), (Dumper $expected), $t);
	}
}

## Check utf-8 in and out of the database

SKIP: {
	eval { require Encode; };
	skip ('Encode module is needed for unicode tests', 14) if $@;

	my $server_encoding = $dbh->selectall_arrayref('SHOW server_encoding')->[0][0];
	skip ('Cannot reliably test unicode without a UTF8 database', 14)
		if $server_encoding ne 'UTF8';

	$t='String should be UTF-8';
	local $dbh->{pg_enable_utf8} = 1;
	my $utf8_str = chr(0x100).'dam'; # LATIN CAPITAL LETTER A WITH MACRON
    ok (Encode::is_utf8( $utf8_str ), $t);

	$t='quote() handles utf8';
	my $quoted = $dbh->quote($utf8_str);
	is ($quoted, qq{'$utf8_str'}, $t);

	$t='Quoted string should be UTF-8';
    ok (Encode::is_utf8( $quoted ), $t);

	$t='quote() handles utf8 inside array';
	$quoted = $dbh->quote([$utf8_str, $utf8_str]);
	is ($quoted, qq!'{"$utf8_str","$utf8_str"}'!, $t);

	$t='Quoted array of strings should be UTF-8';
    ok (Encode::is_utf8( $quoted ), $t);

	## Workaround for client encodings such as SJIS
	my $old_encoding = $dbh->selectall_arrayref('SHOW client_encoding')->[0][0];
	if ($old_encoding ne 'UTF8') {
		$dbh->do(q{SET NAMES 'UTF8'});
	}

	$t='Inserting utf-8 into an array via quoted do() works';
	$dbh->do('DELETE FROM dbd_pg_test');
	$SQL = qq{INSERT INTO dbd_pg_test (id, testarray, val) VALUES (1, $quoted, 'one')};
	eval {
		$dbh->do($SQL);
	};
	is ($@, q{}, $t);

	$t='Retreiving an array containing utf-8 works';
	$SQL = q{SELECT id, testarray, val FROM dbd_pg_test WHERE id = 1};
	$sth = $dbh->prepare($SQL);
	$sth->execute();
	$result = $sth->fetchall_arrayref()->[0];
	my $expected = [1,[$utf8_str,$utf8_str],'one'];
	is_deeply ($result, $expected, $t);

	$t='Selected string should be UTF-8';
    ok (Encode::is_utf8( $result->[1][0] ), $t);

	$t='Selected string should be UTF-8';
    ok (Encode::is_utf8( $result->[1][1] ), $t);

	$t='Inserting utf-8 into an array via prepare and arrayref works';
	$dbh->do('DELETE FROM dbd_pg_test');
	$SQL = q{INSERT INTO dbd_pg_test (id, testarray, val) VALUES (?, ?, 'one')};
	$sth = $dbh->prepare($SQL);
	eval {
		$sth->execute(1,['Bob',$utf8_str]);
	};
	is ($@, q{}, $t);

	$t='Retreiving an array containing utf-8 works';
	$SQL = q{SELECT id, testarray, val FROM dbd_pg_test WHERE id = 1};
	$sth = $dbh->prepare($SQL);
	$sth->execute();
	$result = $sth->fetchall_arrayref()->[0];
	$expected = [1,['Bob',$utf8_str],'one'];
	is_deeply ($result, $expected, $t);

	$t='Selected ASCII string should not be UTF-8';
    ok (!Encode::is_utf8( $result->[1][0] ), $t);

	$t='Selected string should be UTF-8';
    ok (Encode::is_utf8( $result->[1][1] ), $t);

	$t='Non utf-8 inside an array is not return as utf-8';
	$dbh->do('DELETE FROM dbd_pg_test');
	$SQL = q{INSERT INTO dbd_pg_test (id, testarray, val) VALUES (1, '{"noutfhere"}', 'one')};
	$dbh->do($SQL);
	$SQL = q{SELECT testarray FROM dbd_pg_test WHERE id = 1};
	$sth = $dbh->prepare($SQL);
	$sth->execute();
	$result = $sth->fetchall_arrayref()->[0][0];
	ok (!Encode::is_utf8($result), $t);
	$sth->finish();
}


## Quick test of empty arrays
my $expected = $pgversion >= 80300 ? [[[]]] : [[undef]];

$t=q{Empty int array is returned properly};
$result = $dbh->selectall_arrayref(q{SELECT array(SELECT 12345::int WHERE 1=0)::int[]});
is_deeply ($result, $expected, $t);

$t=q{Empty text array is returned properly};
$result = $dbh->selectall_arrayref(q{SELECT array(SELECT 'empty'::text WHERE 1=0)::text[]});
is_deeply ($result, $expected, $t);

cleanup_database($dbh,'test');
$dbh->disconnect;
