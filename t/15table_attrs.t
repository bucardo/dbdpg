#!/usr/bin/perl -w -I./t
$| = 1;

use DBI qw(:sql_types);
use Data::Dumper;
use strict;
use Test::More;
if (defined $ENV{DBI_DSN}) {
    plan tests => 11;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

#
# Test the different methods, so are expected to fail.
#

my $sth;

ok($dbh->do("COMMENT ON COLUMN dbd_pg_test.name IS 'Success'"), 'comment on dbd_pg_test_table');

# Testing table_attributes

my $attrs;
eval { local $dbh->{RaiseError} = 1; $attrs = $dbh->func('dbd_pg_test', 'table_attributes') };
ok(!$@, 'basic table_attributes test') or diag $@;

is($attrs->[0]->{NAME},'id','table_attributes ordering');

cmp_ok($attrs->[0]->{NOTNULL}, '==',1,'table_attributes NOTNULL 1');
cmp_ok($attrs->[1]->{NOTNULL}, '==',0,'table_attributes NOTNULL 0');


cmp_ok($attrs->[1]->{SIZE},'==',20,'table_attributes basic SIZE check');

like($attrs->[3]->{CONSTRAINT}
	, qr/\(\(\(score\s+=\s+1(?:::double\s+precision)?\)\s+OR\s+\(score\s+=\s+2(?:::double precision)?\)\)\s+OR\s+\(score\s+=\s+3(?:::double precision)?\)\)/i
	, 'table_attributes constraints');

# Changed check to 7 because constraint is returning.
cmp_ok(scalar @$attrs, '==', 7, 'table_attributes returns expected number of rows');

my $any_comments = grep { (defined $_->{REMARKS}) and ($_->{REMARKS} eq 'Success') and ($_->{NAME} eq 'name')  } @$attrs;
ok($any_comments, 'found comment on column name');

ok($dbh->disconnect, 'Disconnect');

exit(0);

