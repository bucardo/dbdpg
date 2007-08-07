#!perl -w

# Simply test that we can load the DBI and DBD::PG modules,
# Check that we have a valid version returned from the latter

use Test::More tests => 3;
use strict;

## For quick testing, put new tests as 000xxx.t and set this:
if (exists $ENV{DBDPG_QUICKTEST} and $ENV{DBDPG_QUICKTEST}) {
	BAIL_OUT "Stopping due to DBDPG_QUICKTEST being set";
}

BEGIN {
	use_ok('DBI');
	use_ok('DBD::Pg');
};

like( $DBD::Pg::VERSION, qr/^v[\d\._]+$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});

