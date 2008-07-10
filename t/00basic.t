#!perl

## Simply test that we can load the DBI and DBD::Pg modules,
## and that the latter gives a good version

use 5.006;
use strict;
use warnings;
use Test::More tests => 3;
select(($|=1,select(STDERR),$|=1)[1]);

BEGIN {
	use_ok ('DBI') or BAIL_OUT 'Cannot continue without DBI';
	use_ok ('DBD::Pg') or BAIL_OUT 'Cannot continue without DBD::Pg';
}
use DBD::Pg;
like ($DBD::Pg::VERSION, qr/^v?\d+\.\d+\.\d+(?:_\d+)?$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});
