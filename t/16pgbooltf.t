use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 10;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
                       {RaiseError => 1, AutoCommit => 0}
                      );
ok(defined $dbh,
   'connect with transaction'
  );

ok(get('select 1=1') eq 1,'default true is 1');
ok(get('select 1=0') eq 0,'default false is 0');
ok(!defined get('select null+1=null'),'null');

$dbh->{pg_bool_tf}=0;

ok(get('select 1=1') eq 1,'default true is 1');
ok(get('select 1=0') eq 0,'default false is 0');
ok(!defined get('select null+1=null'),'null');

$dbh->{pg_bool_tf}=1;

ok(!defined get('select null+1=null'),'null');

TODO: {

  local $TODO = "Need to fix these boolean tests";

ok(get('select 1=1') eq 't','tf true is t');
ok(get('select 1=0') eq 'f','tf false is f');
}


sub get{
  my($sql)=@_;

  my $sth=$dbh->prepare($sql);
  $sth->execute;
  my($ret)=$sth->fetchrow_array;
  $sth->finish;

  $ret;
}

