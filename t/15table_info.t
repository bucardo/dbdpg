#!/usr/bin/perl -w -I./t
$| = 1;

use DBI qw(:sql_types);
use Data::Dumper;
use strict;
use Test::More;
if (defined $ENV{DBI_DSN}) {
  plan tests => 11;
} else {
  plan skip_all => 'cannot test without DB info';
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

# Test Table Info
eval {
	$sth = $dbh->table_info( undef, undef, undef );
};
ok( defined $sth, "table_info(undef, undef, undef) tested" ) or diag $@;

$sth = undef;

eval { $sth = $dbh->table_info( undef, undef, undef, "VIEW" ) };
ok( defined $sth, "table_info(undef, undef, undef, \"VIEW\") tested" );
$sth = undef;

# Test Table Info Rule 19a
eval { $sth = $dbh->table_info( '%', '', ''); };
ok( defined $sth, "table_info('%', '', '',) tested" );
$sth = undef;

# Test Table Info Rule 19b
eval { $sth = $dbh->table_info( '', '%', ''); };
ok( defined $sth, "table_info('', '%', '',) tested" );
$sth = undef;

# Test Table Info Rule 19c
eval { $sth = $dbh->table_info( '', '', '', '%') };
ok( defined $sth, "table_info('', '', '', '%',) tested" );
$sth = undef;

# Test to see if this database contains any of the defined table types.
eval { $sth = $dbh->table_info( '', '', '', '%'); };
ok( defined $sth, "table_info('', '', '', '%',) tested" );
if ($sth) {
	my $ref; 
	eval { $ref = $sth->fetchall_hashref( 'TABLE_TYPE' ) };
	ok((defined $ref), "fetchall_hashref('TABLE_TYPE')");
	foreach my $type ( sort keys %$ref ) {
		my $tsth = $dbh->table_info( undef, undef, undef, $type );
		ok( defined $tsth, "table_info(undef, undef, undef, $type) tested" );
		$tsth->finish;
	}
	$sth->finish;
}
$sth = undef;

ok($dbh->disconnect, 'Disconnect');




exit(0);

