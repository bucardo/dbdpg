#!perl -w

# Simply test that we can load the DBI and DBD::PG modules,
# and that the latter gives us a VERSION string

use Test::More tests => 3;
use strict;

BEGIN {
    use_ok('DBI');
    use_ok('DBD::Pg');
};

ok( $DBD::Pg::VERSION, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});

