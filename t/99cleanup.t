use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 4;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 1}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

ok($dbh->do(q{DROP TABLE dbd_pg_test}),       'DROP TABLE dbd_pg_test');
ok($dbh->do( "DROP TABLE dbd_pg_col_info"), 'DROP TABLE dbd_pg_col_info');

ok($dbh->disconnect(),
   'disconnect'
  );
