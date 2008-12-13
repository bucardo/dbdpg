#!perl

## Test of placeholders

use 5.006;
use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 45;

my $t='Connect to database for placeholder testing';
isnt ($dbh, undef, $t);

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

# Make sure that quoting works properly.
$t='Quoting works properly';
my $E = $pgversion >= 80100 ? q{E} : q{};
my $quo = $dbh->quote('\\\'?:');
is ($quo, qq{${E}'\\\\''?:'}, $t);

$t='Quoting works with a function call';
# Make sure that quoting works with a function call.
# It has to be in this function, otherwise it doesn't fail the
# way described in https://rt.cpan.org/Ticket/Display.html?id=4996.
sub checkquote {
    my $str = shift;
    return is ($dbh->quote(substr($str, 0, 10)), "'$str'", $t);
}

checkquote('one');
checkquote('two');
checkquote('three');
checkquote('four');

$t='Fetch returns the correct quoted value';
my $sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,pname) VALUES (?, $quo)});
$sth->execute(100);
my $sql = "SELECT pname FROM dbd_pg_test WHERE pname = $quo";
$sth = $dbh->prepare($sql);
$sth->execute();
my ($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with one bind param where none expected fails';
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with ? placeholder works';
$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = ?';
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with :1 placeholder works';
$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = :1';
$sth = $dbh->prepare($sql);
$sth->bind_param(':1', '\\\'?:');
$sth->execute();
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with $1 placeholder works';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = $1 AND pname <> 'foo'};
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with quoted ? fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '?'};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with quoted :1 fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = ':1'};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with quoted ? fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Prepare with large number of parameters works';
## Test large number of placeholders
$sql = 'SELECT 1 FROM dbd_pg_test WHERE id IN (' . '?,' x 300 . '?)';
my @args = map { $_ } (1..301);
$sth = $dbh->prepare($sql);
my $count = $sth->execute(@args);
$sth->finish();
is ($count, 1, $t);

$sth->finish();

## Force client encoding, as we cannot use backslashes in client-only encodings
my $old_encoding = $dbh->selectall_arrayref('SHOW client_encoding')->[0][0];
if ($old_encoding ne 'UTF8') {
	$dbh->do(q{SET NAMES 'UTF8'});
}

$t='Prepare with backslashes inside quotes works';
my $SQL = q{SELECT setting FROM pg_settings WHERE name = 'backslash_quote'};
$count = $dbh->selectall_arrayref($SQL)->[0];
my $backslash = defined $count ? $count->[0] : 0;
my $scs = $dbh->{pg_standard_conforming_strings};
$SQL = $scs ? q{SELECT E'\\'?'} : q{SELECT '\\'?'};
$sth = $dbh->prepare($SQL);
eval {
	$sth->execute();
};
my $expected = $backslash eq 'off' ? qr{unsafe} : qr{};
like ($@, $expected, $t);

$t='Calling do() with non-DML placeholder works';
$sth->finish();
$dbh->commit();
eval {
  $dbh->do(q{SET search_path TO ?}, undef, 'public');
};
is ($@, q{}, $t);

$t='Calling do() with DML placeholder works';
$dbh->commit();
eval {
  $dbh->do(q{SELECT ?::text}, undef, 'public');
};
is ($@, q{}, $t);

$t='Calling do() with invalid crowded placeholders fails cleanly';
$dbh->commit();
eval {
  $dbh->do(q{SELECT ??}, undef, 'public', 'error');
};
is($dbh->state, '42601', $t);

$t='Prepare/execute with non-DML placeholder works';
$dbh->commit();
eval {
  $sth = $dbh->prepare(q{SET search_path TO ?});
  $sth->execute('public');
};
is ($@, q{}, $t);

$t='Prepare/execute does not allow geometric operators';
$dbh->commit();
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
};
like ($@, qr{unbound placeholder}, $t);

$t='Prepare/execute allows geometric operator ?- when dollaronly is set';
$dbh->commit();
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
	$sth->finish();
};
is ($@, q{}, $t);

$t='Prepare/execute allows geometric operator ?# when dollaronly set';
$dbh->commit();
eval {
	$sth = $dbh->prepare(q{SELECT lseg'(1,0),(1,1)' ?# lseg '(2,3),(4,5)'});
	$sth->execute();
	$sth->finish();
};
is ($@, q{}, $t);

$t=q{Value of placeholder_dollaronly can be retrieved};
is ($dbh->{pg_placeholder_dollaronly}, 1, $t);

$t=q{Prepare/execute does not allow use of raw ? and :foo forms};
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1});
	$sth->execute();
	$sth->finish();
};
like ($@, qr{mix placeholder}, $t);

$t='Prepare/execute allows use of raw ? and :foo forms when dollaronly set';
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->{pg_placeholder_dollaronly} = 1;
	$sth->execute();
	$sth->finish();
};
like ($@, qr{unbound placeholder}, $t);

$t='Prepare works with pg_placeholder_dollaronly';
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->execute();
	$sth->finish();
};
like ($@, qr{unbound placeholder}, $t);

$t='Prepare works with identical named placeholders';
eval {
	$sth = $dbh->prepare(q{SELECT :row, :row, :row, :yourboat});
	$sth->finish();
};
is ($@, q{}, $t);

$t='Prepare works with placeholders after double slashes';
eval {
	$dbh->do(q{CREATE OPERATOR // ( PROCEDURE=bit, LEFTARG=int, RIGHTARG=int )});
	$sth = $dbh->prepare(q{SELECT ? // ?});
	$sth->execute(1,2);
	$sth->finish();
};
is ($@, q{}, $t);

$t='Dollar quotes starting with a number are not treated as valid identifiers';
eval {
	$sth = $dbh->prepare(q{SELECT $123$  $123$});
	$sth->execute(1);
	$sth->finish();
};
like ($@, qr{Invalid placeholders}, $t);

$t='Dollar quotes with invalid characters are not parsed as identifiers';
for my $char (qw!+ / : @ [ `!) {
	eval {
		$sth = $dbh->prepare(qq{SELECT \$abc${char}\$ 123 \$abc${char}\$});
		$sth->execute();
		$sth->finish();
	};
	like ($@, qr{syntax error}, $t);
}

$t='Dollar quotes with valid characters are parsed as identifiers';
$dbh->rollback();
for my $char (qw{0 9 A Z a z}) {
	eval {
		$sth = $dbh->prepare(qq{SELECT \$abc${char}\$ 123 \$abc${char}\$});
		$sth->execute();
		$sth->finish();
	};
	is ($@, q{}, $t);
}

$t='Backslash quoting inside double quotes is parsed correctly';
eval {
	$sth = $dbh->prepare(q{SELECT * FROM "\" WHERE a=?});
	$sth->execute(1);
	$sth->finish();
};
like ($@, qr{relation ".*" does not exist}, $t);
$dbh->rollback();

SKIP: {
	skip 'Setting standard_conforming_strings not available', 2 if ! defined $scs;
	$t='Backslash quoting inside single quotes is parsed correctly with standard_conforming_strings off';
	eval {
		$dbh->do(q{SET standard_conforming_strings = 'off'});
		$sth = $dbh->prepare(q{SELECT '\', ?});
		$sth->execute();
		$sth->finish();
	};
	like ($@, qr{unterminated quoted string}, $t);
	$dbh->rollback();

	$t='Backslash quoting inside single quotes is parsed correctly with standard_conforming_strings on';
	eval {
		$dbh->do(q{SET standard_conforming_strings = 'on'});
		$sth = $dbh->prepare(q{SELECT '\', ?::int});
		$sth->execute(1);
		$sth->finish();
	};
	is ($@, q{}, $t);
}

## Begin custom type testing

$dbh->rollback();

cleanup_database($dbh,'test');
$dbh->disconnect();

