#!perl

## Make sure we can connect and disconnect cleanly
## All tests are stopped if we cannot make the first connect

use 5.008001;
use strict;
use warnings;
use DBI;
use DBD::Pg;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

## Define this here in case we get to the END block before a connection is made.
BEGIN {
    use vars qw/$t $pgversion $pglibversion $pgvstring $pgdefport $helpconnect $dbh $connerror %set/;
    ($pgversion,$pglibversion,$pgvstring,$pgdefport) = ('?','?','?','?');
}

($helpconnect,$connerror,$dbh) = connect_database();

if (! defined $dbh or $connerror) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 15;

pass ('Established a connection to the database');

$pgversion    = $dbh->{pg_server_version};
$pglibversion = $dbh->{pg_lib_version};
$pgdefport    = $dbh->{pg_default_port};
$pgvstring    = $dbh->selectall_arrayref('SELECT VERSION()')->[0][0];

ok ($dbh->disconnect(), 'Disconnect from the database');

# Connect two times. From this point onward, do a simpler connection check
$t=q{Second database connection attempt worked};
(undef,$connerror,$dbh) = connect_database();
is ($connerror, '', $t);
if ($connerror ne '') {
    BAIL_OUT 'Second connection to database failed, bailing out';
}

## Grab some important values used for debugging
my @vals = qw/array_nulls backslash_quote server_encoding client_encoding standard_conforming_strings/;
my $SQL = 'SELECT name,setting FROM pg_settings WHERE name IN (' .
    (join ',' => map { qq{'$_'} } @vals) . ')';
for (@{$dbh->selectall_arrayref($SQL)}) {
    $set{$_->[0]} = $_->[1];
}

my $dbh2 = connect_database();

pass ('Connected with second database handle');

my $sth = $dbh->prepare('SELECT 123');
ok ($dbh->disconnect(), 'Disconnect with first database handle');
ok ($dbh2->disconnect(), 'Disconnect with second database handle');
ok ($dbh2->disconnect(), 'Disconnect again with second database handle');

eval {
 $sth->execute();
};
ok ($@, 'Execute fails on a disconnected statement');

# Try out various connection options
$ENV{DBI_DSN} ||= '';
SKIP: {
    my $alias = qr{(database|db|dbname)};
    if ($ENV{DBI_DSN} !~ /$alias\s*=\s*\S+/) {
        skip ('DBI_DSN contains no database option, so skipping connection tests', 7);
    }

    $t=q{Connect with invalid option fails};
    my $err;
    (undef,$err,$dbh) = connect_database({ dbreplace => 'dbbarf', nocreate => 1 });
    like ($err, qr{DBI connect.+failed:}, $t);

    for my $opt (qw/db dbname database/) {
        $t=qq{Connect using string '$opt' works};
        $dbh and $dbh->disconnect();
        (undef,$err,$dbh) = connect_database({dbreplace => $opt});
        $err =~ s/(Previous failure).*/$1/;
        is ($err, '', $t);
    }

    $t=q{Connect with forced uppercase 'DBI:' works};
    my ($testdsn,$testuser,$helpconnect,$su,$uid,$testdir,$pg_ctl,$initdb,$error,$version)
        = get_test_settings();
    $testdsn =~ s/^dbi/DBI/i;
    my $ldbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
        {RaiseError => 1, PrintError => 0, AutoCommit => 0});
    ok (ref $ldbh, $t);
    $ldbh->disconnect();

    $t=q{Connect with mixed case 'DbI:' works};
    $testdsn =~ s/^dbi/DbI/i;
    $ldbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
        {RaiseError => 1, PrintError => 0, AutoCommit => 0});
    ok (ref $ldbh, $t);
    $ldbh->disconnect();

    if ($ENV{DBI_DSN} =~ /$alias\s*=\s*\"/) {
        skip ('DBI_DSN already contains quoted database, no need for explicit test', 1);
    }
    $t=q{Connect using a quoted database argument};
    eval {
        $dbh and $dbh->disconnect();
        (undef,$err,$dbh) = connect_database({dbquotes => 1, nocreate => 1});
    };
    is ($@, q{}, $t);
}

END {
    my $pv = sprintf('%vd', $^V);
    my $schema = 'dbd_pg_testschema';
    my $dsn = exists $ENV{DBI_DSN} ? $ENV{DBI_DSN} : '?';

    ## Don't show current dir to the world via CPAN::Reporter results
    $dsn =~ s{host=/.*(dbdpg_test_database/data/socket)}{host=<pwd>/$1};

    my $ver = defined $DBD::Pg::VERSION ? $DBD::Pg::VERSION : '?';
    my $user = exists $ENV{DBI_USER} ? $ENV{DBI_USER} : '<not set>';
    my $offset = 27;

    my $extra = '';
    for (sort qw/HOST HOSTADDR PORT DATABASE USER PASSWORD PASSFILE OPTIONS REALM
                 REQUIRESSL KRBSRVNAME CONNECT_TIMEOUT SERVICE SSLMODE SYSCONFDIR
                 CLIENTENCODING/) {
        my $name = "PG$_";
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
        }
    }
    for my $name (qw/DBI_DRIVER DBI_AUTOPROXY LANG/) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
        }
    }

    ## More helpful stuff
    for (sort keys %set) {
        $extra .= sprintf "\n%-*s %s", $offset, $_, $set{$_};
    }

    if ($helpconnect) {
        $extra .= sprintf "\n%-*s ", $offset, 'Adjusted:';
        if ($helpconnect & 1) {
            $extra .= 'DBI_DSN ';
        }
        if ($helpconnect & 4) {
            $extra .= 'DBI_USER';
        }
        if ($helpconnect & 8) {
            $extra .= 'DBI_USERx2';
        }
        if ($helpconnect & 16) {
            $extra .= 'initdb';
        }
    }

    if (defined $connerror and length $connerror) {
        $connerror =~ s/.+?failed: ([^\n]+).*/$1/s;
        $connerror =~ s{\n at t/dbdpg.*}{}m;
        if ($connerror =~ /create semaphores/) {
            $connerror =~ s/.*(FATAL.*?)HINT.*/$1/sm;
        }
        $extra .= "\nError was: $connerror";
    }

    diag
        "\nDBI                         Version $DBI::VERSION\n".
        "DBD::Pg                     Version $ver\n".
        "Perl                        Version $pv\n".
        "OS                          $^O\n".
        "PostgreSQL (compiled)       $pglibversion\n".
        "PostgreSQL (target)         $pgversion\n".
        "PostgreSQL (reported)       $pgvstring\n".
        "Default port                $pgdefport\n".
        "DBI_DSN                     $dsn\n".
        "DBI_USER                    $user\n".
        "Test schema                 $schema$extra\n";
}
