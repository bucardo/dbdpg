#!perl

## Test of placeholders

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
plan tests => 27;

my $t=q{Connect to database for placeholder testing};
isnt( $dbh, undef, $t);

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

# Make sure that quoting works properly.
my $E = $pgversion >= 80100 ? q{E} : q{};
$t=q{Quoting works properly};
my $quo = $dbh->quote('\\\'?:');
is( $quo, qq{${E}'\\\\''?:'}, $t);

$t=q{Quoting works with a function call};
# Make sure that quoting works with a function call.
# It has to be in this function, otherwise it doesn't fail the
# way described in https://rt.cpan.org/Ticket/Display.html?id=4996.
sub checkquote {
    my $str = shift;
    return is( $dbh->quote(substr($str, 0, 10)), "'$str'", $t);
}

checkquote('one');
checkquote('two');
checkquote('three');
checkquote('four');

my $sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,pname) VALUES (?, $quo)});
$sth->execute(100);

my $sql = "SELECT pname FROM dbd_pg_test WHERE pname = $quo";
$sth = $dbh->prepare($sql);
$sth->execute();

$t=q{Fetch returns the correct quoted value};
my ($retr) = $sth->fetchrow_array();
is( $retr, '\\\'?:', $t);

$t=q{Execute with one bind param where none expected fails};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like( $@, qr{when 0 are needed}, $t);

$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = ?';
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');

$t=q{Execute with ? placeholder works};
($retr) = $sth->fetchrow_array();
is( $retr, '\\\'?:', $t);

$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = :1';
$sth = $dbh->prepare($sql);
$sth->bind_param(':1', '\\\'?:');
$sth->execute();

$t=q{Execute with :1 placeholder works};
($retr) = $sth->fetchrow_array();
is( $retr, '\\\'?:', $t);

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = $1 AND pname <> 'foo'};
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');

$t=q{Execute with $1 placeholder works};
($retr) = $sth->fetchrow_array();
is( $retr, '\\\'?:', $t);

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '?'};

$t=q{Execute with quoted ? fails with a placeholder};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like( $@, qr{when 0 are needed}, $t);

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = ':1'};

$t=q{Execute with quoted :1 fails with a placeholder};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like( $@, qr{when 0 are needed}, $t);

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'};

$t=q{Execute with quoted ? fails with a placeholder};
eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
like( $@, qr{when 0 are needed}, $t);

$t=q{Prepare with large number of parameters works};
## Test large number of placeholders
$sql = 'SELECT 1 FROM dbd_pg_test WHERE id IN (' . '?,' x 300 . '?)';
my @args = map { $_ } (1..301);
$sth = $dbh->prepare($sql);
my $count = $sth->execute(@args);
$sth->finish();
is( $count, 1, $t);

$sth->finish();

## Force client encoding, as we cannot use backslashes in client-only encodings
my $old_encoding = $dbh->selectall_arrayref('SHOW client_encoding')->[0][0];
if ($old_encoding ne 'UTF8') {
	$dbh->do(q{SET NAMES 'UTF8'});
}

my $SQL = q{SELECT setting FROM pg_settings WHERE name = 'backslash_quote'};
$count = $dbh->selectall_arrayref($SQL)->[0];
my $backslash = defined $count ? $count->[0] : 0;

$t=q{Prepare with backslashes inside quotes works};
my $scs = $dbh->{pg_standard_conforming_strings};
$SQL = $scs ? q{SELECT E'\\'?'} : q{SELECT '\\'?'};
$sth = $dbh->prepare($SQL);
eval {
	$sth->execute();
};
my $expected = $backslash eq 'off' ? qr{unsafe} : qr{};
like( $@, $expected, $t);
$sth->finish();
$dbh->commit();

$t=q{Calling do() with non-DML placeholder works};
eval {
  $dbh->do(q{SET search_path TO ?}, undef, 'public');
};
is( $@, q{}, $t);
$dbh->commit();

$t=q{Calling do() with DML placeholder works};
eval {
  $dbh->do(q{SELECT ?::text}, undef, 'public');
};
is( $@, q{}, $t);
$dbh->commit();

$t=q{Prepare/execute with non-DML placehlder works};
eval {
  $sth = $dbh->prepare(q{SET search_path TO ?});
  $sth->execute('public');
};
is( $@, q{}, $t);
$dbh->commit();

$t=q{Prepare/execute does not allow geometric operators};
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
};
like( $@, qr{unbound placeholder}, $t);
$dbh->commit();

$t=q{Prepare/execute allows geometric operator ?- when dollaronly is set};
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
	$sth->finish();
};
is( $@, q{}, $t);
$dbh->commit();


$t=q{Prepare/execute allows geometric operator ?# when dollaronly set};
eval {
	$sth = $dbh->prepare(q{SELECT lseg'(1,0),(1,1)' ?# lseg '(2,3),(4,5)'});
	$sth->execute();
	$sth->finish();
};
is( $@, q{}, $t);

$t=q{Value of placeholder_dollaronly can be retrieved};
is( $dbh->{pg_placeholder_dollaronly}, 1, $t);

$t=q{Prepare/execute does not allow use of raw ? and :foo forms};
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1});
	$sth->execute();
	$sth->finish();
};
like( $@, qr{mix placeholder}, $t);

$t=q{Prepare/execute allows use of raw ? and :foo forms when dollaronly set};
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->{pg_placeholder_dollaronly} = 1;
	$sth->execute();
	$sth->finish();
};
like( $@, qr{unbound placeholder}, $t);

$t=q{Prepare works with pg_placeholder_dollaronly};
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->execute();
	$sth->finish();
};
like( $@, qr{unbound placeholder}, $t);

$t=q{Prepare works with identical named placeholders};
eval {
	$sth = $dbh->prepare(q{SELECT :row, :row, :row, :yourboat});
	$sth->finish();
};
is( $@, q{}, $t);

$dbh->rollback();

cleanup_database($dbh,'test');
$dbh->disconnect();

