#!perl

## Make sure we can connect and disconnect cleanly
## All tests are stopped if we cannot make the first connect

use strict;
use warnings;
use DBI;
use DBD::Pg;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

## Define this here in case we get to the END block before a connection is made.
BEGIN {
	use vars qw/$pgversion $pglibversion $pgvstring $pgdefport $helpconnect $dbh $connerror %set/;
	($pgversion,$pglibversion,$pgvstring,$pgdefport) = ('?','?','?','?');
}

($helpconnect,$connerror,$dbh) = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 15;

# Trapping a connection error can be tricky, but we only have to do it
# this thoroughly one time. We are trapping two classes of errors:
# the first is when we truly do not connect, usually a bad DBI_DSN;
# the second is an invalid login, usually a bad DBI_USER or DBI_PASS

my ($t);

pass('Established a connection to the database');

$pgversion    = $dbh->{pg_server_version};
$pglibversion = $dbh->{pg_lib_version};
$pgdefport    = $dbh->{pg_default_port};
$pgvstring    = $dbh->selectall_arrayref('SELECT VERSION()')->[0][0];

ok( $dbh->disconnect(), 'Disconnect from the database');

# Connect two times. From this point onward, do a simpler connection check
$dbh = connect_database();

pass('Connected with first database handle');

## Grab some important values used for debugging
my @vals = qw/array_nulls backslash_quote server_encoding client_encoding standard_confirming_strings/;
my $SQL = 'SELECT name,setting FROM pg_settings WHERE name IN (' .
	(join ',' => map { qq{'$_'} } @vals) . ')';
for (@{$dbh->selectall_arrayref($SQL)}) {
	$set{$_->[0]} = $_->[1];
}

my $dbh2 = connect_database();

pass('Connected with second database handle');

my $sth = $dbh->prepare('SELECT 123');
ok ( $dbh->disconnect(), 'Disconnect with first database handle');
ok ( $dbh2->disconnect(), 'Disconnect with second database handle');
ok ( $dbh2->disconnect(), 'Disconnect again with second database handle');

eval {
 $sth->execute();
};
ok( $@, 'Execute fails on a disconnected statement');

# Try out various connection options
$ENV{DBI_DSN} ||= '';
SKIP: {
	my $alias = qr{(database|db|dbname)};
	if ($ENV{DBI_DSN} !~ /$alias\s*=\s*\S+/) {
		skip 'DBI_DSN contains no database option, so skipping connection tests', 5;
	}

	$t=q{Connect with invalid option fails};
	my $oldname = $1;
	(my $dbi = $ENV{DBI_DSN}) =~ s/$alias\s*=/dbbarf=/;
	eval {
		$dbh = DBI->connect($dbi, $ENV{DBI_USER}, $ENV{DBI_PASS}, {RaiseError=>1});
	};
	like ($@, qr{DBI connect.+failed:}, $t);
	for my $opt (qw/db dbname database/) {
		$t=qq{Connect using string '$opt' works};
		($dbi = $ENV{DBI_DSN}) =~ s/$alias\s*=/$opt=/;
		eval {
			$dbh = DBI->connect($dbi, $ENV{DBI_USER}, $ENV{DBI_PASS}, {RaiseError=>1});
		};
		is($@, q{}, $t);
	}

	if ($ENV{DBI_DSN} =~ /$alias\s*=\s*\"/) {
		skip 'DBI_DSN already contains quoted database, no need for explicit test', 1;
	}
	$t=q{Connect using a quoted database argument};
	($dbi = $ENV{DBI_DSN}) =~ s/$alias\s*=(\w+)/'db="'.lc $2.'"'/e;
	eval {
		$dbh = DBI->connect($dbi, $ENV{DBI_USER}, $ENV{DBI_PASS}, {RaiseError=>1});
	};
	is($@, q{}, $t);
}

$t=q{Connect with an undefined user picks up $ENV{DBI_USER}};
eval {
	$dbh = DBI->connect($ENV{DBI_DSN}, undef, $ENV{DBI_PASS}, {RaiseError=>1});
};
is($@, q{}, $t);

$t=q{Connect with an undefined password picks up $ENV{DBI_PASS}};
eval {
	$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, undef, {RaiseError=>1});
};
is($@, q{}, $t);

END {
	my $pv = sprintf('%vd', $^V);
	my $schema = 'dbd_pg_testschema';
	my $dsn = exists $ENV{DBI_DSN} ? $ENV{DBI_DSN} : '?';
	my $ver = defined $DBD::Pg::VERSION ? $DBD::Pg::VERSION : '?';
	my $user = exists $ENV{DBI_USER} ? $ENV{DBI_USER} : '<not set>';

	my $extra = '';
	for (sort qw/HOST HOSTADDR PORT DATABASE USER PASSWORD PASSFILE OPTIONS REALM
                 REQUIRESSL KRBSRVNAME CONNECT_TIMEOUT SERVICE SSLMODE SYSCONFDIR
                 CLIENTENCODING/) {
		my $name = "PG$_";
		if (exists $ENV{$name} and defined $ENV{$name}) {
			$extra .= sprintf "\n%-21s $ENV{$name}", $name;
		}
	}
	for my $name (qw/DBI_DRIVER DBI_AUTOPROXY/) {
		if (exists $ENV{$name} and defined $ENV{$name}) {
			$extra .= sprintf "\n%-21s $ENV{$name}", $name;
		}
	}

	## More helpful stuff
	for (sort keys %set) {
		$extra .= sprintf "\n%-21s %s", $_, $set{$_};
	}

	if ($helpconnect) {
		$extra .= "\nAdjusted:             ";
		if ($helpconnect & 1) {
			$extra .= 'DBI_DSN ';
		}
		if ($helpconnect & 4) {
			$extra .= 'DBI_USER';
		}
	}

	if (defined $connerror) {
		$connerror =~ s/.+?failed: //;
		$connerror =~ s{\n at t/dbdpg.*}{}m;
		$extra .= "\nError was: $connerror";
	}

	diag
		"\nDBI                   Version $DBI::VERSION\n".
		"DBD::Pg               Version $ver\n".
		"Perl                  Version $pv\n".
		"OS                    $^O\n".
		"PostgreSQL (compiled) $pglibversion\n".
		"PostgreSQL (target)   $pgversion\n".
		"PostgreSQL (reported) $pgvstring\n".
		"Default port          $pgdefport\n".
		"DBI_DSN               $dsn\n".
		"DBI_USER              $user\n".
		"Test schema           $schema$extra\n";
}
