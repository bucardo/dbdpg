use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 3;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 1}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

ok($dbh->do(q{DROP TABLE dbd_pg_test}),
   'drop'
  );

ok($dbh->disconnect(),
   'disconnect'
  );
