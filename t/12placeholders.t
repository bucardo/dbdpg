#!perl

## Test of placeholders

use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (defined $dbh) {
	plan tests => 26;
}
else {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

ok( defined $dbh, 'Connect to database for placeholder testing');

my ($pglibversion,$pgversion) = ($dbh->{pg_lib_version},$dbh->{pg_server_version});
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

# Make sure that quoting works properly.
my $quo = $dbh->quote('\\\'?:');
is( $quo, q{'\\\\''?:'}, 'Properly quoted');

# Make sure that quoting works with a function call.
# It has to be in this function, otherwise it doesn't fail the
# way described in https://rt.cpan.org/Ticket/Display.html?id=4996.
sub checkquote {
    my $str = shift;
    return is( $dbh->quote(substr($str, 0, 10)), "'$str'", 'First function quote');
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

my ($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq '\\\'?:'), 'fetch');

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with one bind param where none expected');

$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = ?';
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq '\\\'?:'), 'execute with ? placeholder');

$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = :1';
$sth = $dbh->prepare($sql);
$sth->bind_param(':1', '\\\'?:');
$sth->execute();

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq '\\\'?:'), 'execute with :1 placeholder');

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = $1 AND pname <> 'foo'};
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq '\\\'?:'), 'execute with $1 placeholder');

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '?'};

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = ':1'};

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted :1');

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'};
$sth = $dbh->prepare($sql);

eval {
## XX ???
	local $dbh->{PrintError} = 0;
	local $sth->{PrintError} = 0;
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

## Test large number of placeholders
$sql = 'SELECT 1 FROM dbd_pg_test WHERE id IN (' . '?,' x 300 . '?)';
my @args = map { $_ } (1..301);
$sth = $dbh->prepare($sql);
my $count = $sth->execute(@args);
$sth->finish();
ok( $count >= 1, 'prepare with large number of parameters works');

$sth->finish();

## Test our parsing of backslashes
$sth = $dbh->prepare(q{SELECT '\\'?'});
eval {
	$sth->execute();
};
ok( !$@, 'prepare with backslashes inside quotes works');
$sth->finish();
$dbh->commit();

## Test do() with placeholders, both DML and non-DML
eval {
  $dbh->do(q{SET search_path TO ?}, undef, 'public');
};
ok( !$@, 'do() called with non-DML placeholder works');
$dbh->commit();

eval {
  $dbh->do(q{SELECT ?::text}, undef, 'public');
};
ok( !$@, 'do() called with non-DML placeholder works');
$dbh->commit();

## Test a non-DML placeholder
eval {
  $sth = $dbh->prepare(q{SET search_path TO ?});
  $sth->execute('public');
};
ok( !$@, 'prepare/execute with non-DML placeholder works');
$dbh->commit();


## Make sure we can allow geometric and other placeholders
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
};
like ($@, qr{unbound placeholder}, q{prepare/execute does not allows geometric operators});
$dbh->commit();

$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
	$sth->execute();
	$sth->finish();
};
is ($@, q{}, q{prepare/execute allows geometric operator ?- when dollaronly set});
$dbh->commit();

eval {
	$sth = $dbh->prepare(q{SELECT lseg'(1,0),(1,1)' ?# lseg '(2,3),(4,5)'});
	$sth->execute();
	$sth->finish();
};
is ($@, q{}, q{prepare/execute allows geometric operator ?# when dollaronly set});

is ($dbh->{pg_placeholder_dollaronly}, 1, q{Value of placeholder_dollaronly can be retrieved});

$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1});
	$sth->execute();
	$sth->finish();
};
like ($@, qr{mix placeholder}, q{prepare/execute does not allow use of raw ? and :foo forms});

$dbh->{pg_placeholder_dollaronly} = 1;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->{pg_placeholder_dollaronly} = 1;
	$sth->execute();
	$sth->finish();
};
like ($@, qr{unbound placeholder}, q{prepare/execute allows use of raw ? and :foo forms when dollaronly set});

$dbh->{pg_placeholder_dollaronly} = 0;
eval {
	$sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
	$sth->execute();
	$sth->finish();
};
like ($@, qr{unbound placeholder}, q{pg_placeholder_dollaronly can be called as part of prepare()});

$dbh->rollback();

cleanup_database($dbh,'test');
$dbh->disconnect();

