#!perl -w

# Test of placeholders

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 17;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											 {RaiseError => 1, PrintError => 0, AutoCommit => 0});
ok( defined $dbh, 'Connect to database for placeholder testing');

if (DBD::Pg::_pg_use_catalog($dbh)) {
	$dbh->do("SET search_path TO " . $dbh->quote_identifier
					 (exists $ENV{DBD_SCHEMA} ? $ENV{DBD_SCHEMA} : 'public'));
}

# Make sure that quoting works properly.
my $quo = $dbh->quote("\\'?:");
is( $quo, "'\\\\''?:'", "Properly quoted");

# Make sure that quoting works with a function call.
# It has to be in this function, otherwise it doesn't fail the
# way described in https://rt.cpan.org/Ticket/Display.html?id=4996.
sub checkquote {
    my $str = shift;
    is( $dbh->quote(substr($str, 0, 10)), "'$str'", "First function quote");
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
ok( (defined($retr) && $retr eq "\\'?:"), "fetch");

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with one bind param where none expected');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = ?";
$sth = $dbh->prepare($sql);
$sth->execute("\\'?:");

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'execute with ? placeholder');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = :1";
$sth = $dbh->prepare($sql);
$sth->bind_param(":1", "\\'?:");
$sth->execute();

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'execute with :1 placeholder');

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = $1 AND pname <> 'foo'};
$sth = $dbh->prepare($sql);
$sth->execute("\\'?:");

($retr) = $sth->fetchrow_array();
ok( (defined($retr) && $retr eq "\\'?:"), 'execute with $1 placeholder');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = '?'";

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = ':1'";

eval {
	$sth = $dbh->prepare($sql);
	$sth->execute('foo');
};
ok( $@, 'execute with quoted :1');

$sql = "SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'";
$sth = $dbh->prepare($sql);

eval {
## XX ???
	local $dbh->{PrintError} = 0;
	local $sth->{PrintError} = 0;
	$sth->execute('foo');
};
ok( $@, 'execute with quoted ?');

## Test large number of placeholders
$sql = 'SELECT 1 FROM dbd_pg_test WHERE id IN (' . '?,' x 300 . "?)";
my @args = map { $_ } (1..301);
$sth = $dbh->prepare($sql);
my $count = $sth->execute(@args);
$sth->finish();
ok( $count >= 1, 'prepare with large number of parameters works');

$sth->finish();

## Test our parsing of backslashes
$sth = $dbh->prepare("SELECT '\\'?'");
eval {
	$sth->execute();
};
ok(!$@, 'prepare with backslashes inside quotes works');
$sth->finish();

$dbh->rollback();

ok( $dbh->disconnect(), 'Disconnect from database');

