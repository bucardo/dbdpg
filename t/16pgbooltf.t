use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 10;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
                       {RaiseError => 1, AutoCommit => 0}
                      );
ok(defined $dbh,
   'connect with transaction'
  );

ok(get('select 1=1') eq 1,'default true is 1');
ok(get('select 1=0') eq 0,'default false is 0');
ok(!defined get('select 1=null'),'null');

$dbh->{pg_bool_tf}=0;

ok(get('select 1=1') eq 1,'default true is 1');
ok(get('select 1=0') eq 0,'default false is 0');
ok(!defined get('select 1=null'),'null');

$dbh->{pg_bool_tf}=1;

#rl: TODO: FIX.

SKIP: {
skip("broken",3);
ok(get('select 1=1') eq 't','tf true is t');
ok(get('select 1=0') eq 'f','tf false is f');
ok(!defined get('select 1=null'),'null');
}


sub get{
  my($sql)=@_;

  my $sth=$dbh->prepare($sql);
  $sth->execute;
  my($ret)=$sth->fetchrow_array;
  $sth->finish;

  $ret;
}

