use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
	plan tests => 2;
} else {
	plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh;
eval {
$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
	{RaiseError => 0, PrintError => 0, AutoCommit => 0});
};

ok((defined $dbh and $dbh->disconnect()),
	'connect with transaction'
) or print STDOUT "Bail out! Could not connect to the database.\n";

undef $dbh;
$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
	{RaiseError => 1, AutoCommit => 1});

ok((defined $dbh),
	'connect without transaction'
);

# Some helpful diagnostics for debugging and bug and test reporting
diag "\nPackage       Version";
diag "DBD::Pg       $DBD::Pg::VERSION";
diag "DBI           $DBI::VERSION"; 
diag "Postgres      ".DBD::Pg::_pg_server_version($dbh);

$dbh->disconnect();
