#!perl

## Simply test that we can load the DBI and DBD::Pg modules,
## and that the latter gives a good version

use strict;
use warnings;
use Test::More tests => 4;
select(($|=1,select(STDERR),$|=1)[1]);

BEGIN {
	use_ok('DBI') or BAIL_OUT 'Cannot continue without DBI';
	use_ok('DBD::Pg') or BAIL_OUT 'Cannot continue without DBD::Pg';
}
use DBD::Pg;
like( $DBD::Pg::VERSION, qr/^v?\d+\.\d+\.\d+(?:_\d+)?$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});

SKIP: {
	eval { require Test::Warn; };
	$@ and skip 'Need Test::Warn to test version warning', 1;

	my $t=q{Version comparison does not throw a warning};

	Test::Warn::warnings_are (sub {$DBD::Pg::VERSION <= '1.49'}, [], $t );
}
