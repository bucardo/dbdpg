use Test::More tests => 3;
BEGIN {
    use_ok('DBI');
    use_ok('DBD::Pg');
};

ok($DBD::Pg::VERSION, 'found DBD::Pg::VERSION');
