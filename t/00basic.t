#!perl -w

# Simply test that we can load the DBI and DBD::PG modules,
# Check that we have a valid version returned from the latter

use Test::More tests => 3;
use strict;

BEGIN {
	use_ok('DBI');
	use_ok('DBD::Pg');
};

like( $DBD::Pg::VERSION, qr/^[\d\._]+$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});

