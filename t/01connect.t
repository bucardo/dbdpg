#!perl -w

# Make sure we can connect and disconnect cleanly
# All tests are stopped if we cannot make the first connect

use Test::More;
use DBI;
use strict;
$|=1;

if (defined $ENV{DBI_DSN}) {
	plan tests => 8;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file.';
}

## Define this here in case we get to the END block before a connection is made.
my $pgversion = '?';

# Trapping a connection error can be tricky, but we only have to do it 
# this thoroughly one time. We are trapping two classes of errors:
# the first is when we truly do not connect, usually a bad DBI_DSN;
# the second is an invalid login, usually a bad DBI_USER or DBI_PASS

my $dbh;
eval {
	$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											{RaiseError => 1, PrintError => 0, AutoCommit => 0});
};
if ($@) {
	if (! $DBI::errstr) {
		print STDOUT "Bail out! Could not connect: $@\n";
	}
	else {
		print STDOUT "Bail out! Could not connect: $DBI::errstr\n";
	}
	exit; # Force a hasty exit
}

pass('Established a connection to the database');

$pgversion = DBD::Pg::_pg_server_version($dbh);

ok( $dbh->disconnect(), 'Disconnect from the database');

# Connect two times. From this point onward, do a simpler connection check
ok( $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
												{RaiseError => 1, PrintError => 0, AutoCommit => 0}),
		'Connected with first database handle');

my $dbh2;
ok( $dbh2 = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
												 {RaiseError => 1, PrintError => 0, AutoCommit => 0}),
		'Connected with second database handle');

my $sth = $dbh->prepare('SELECT * FROM dbd_pg_test');
ok ( $dbh->disconnect(), 'Disconnect with first database handle');
ok ( $dbh2->disconnect(), 'Disconnect with second database handle');
ok ( $dbh2->disconnect(), 'Disconnect again with second database handle');

eval {
 $sth->execute();
};
ok( $@, 'Execute fails on a disconnected statement');

END {
	my $pv = sprintf("%vd", $^V);
	my $schema = exists $ENV{DBD_SCHEMA} ? 
		"\nDBD_SCHEMA        $ENV{DBD_SCHEMA}" : '';
	diag 
		"\nProgram           Version\n".
		"Perl              $pv ($^O)\n".
		"DBD::Pg           $DBD::Pg::VERSION\n".
		"PostgreSQL        $pgversion\n".
		"DBI               $DBI::VERSION\n".
		"DBI_DSN           $ENV{DBI_DSN}$schema\n";
}
