#!perl

## Make sure we can connect and disconnect cleanly
## All tests are stopped if we cannot make the first connect

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use utf8; ## no critic (TooMuchCode::ProhibitUnnecessaryUTF8Pragma)
use DBI;
use DBD::Pg;
use Test::More;
my $verbose_end = 0;
require 'dbdpg_test_setup.pl';
$verbose_end = 1;
select(($|=1,select(STDERR),$|=1)[1]);

## Define this here in case we get to the END block before a connection is made.
our ($t, $pgversion, $pglibversion, $pgvstring, $pgdefport, $helpconnect, $dbh, $connerror, %setting);
BEGIN {
    ($pgversion,$pglibversion,$pgvstring,$pgdefport) = ('?','?','?','?');
}

eval {
    ($helpconnect,$connerror,$dbh) = connect_database();
};
if ($@ =~ /Invalid initdb/) {
    BAIL_OUT 'Could not connect: no initdb found';
}

$connerror = '' if !defined $connerror;
if ($connerror or (!defined $dbh)) {
    plan skip_all => "Connection to database failed, cannot continue testing ($connerror) (dbh=" . (defined($dbh) ? $dbh : '<undefined>') . ')';
}

plan tests => 24;

pass ('Connection to test database works');

$pgversion    = $dbh->{pg_server_version};
$pglibversion = $dbh->{pg_lib_version};
$pgvstring    = $dbh->selectall_arrayref('SELECT VERSION()')->[0][0];
$pgdefport    = $dbh->{pg_default_port};

ok ($dbh->disconnect(), 'Calling $dbh->disconnect() works');

# Connect two times. From this point onward, do a simpler connection check
$t=q{Second database connection works};
(undef,$connerror,$dbh) = connect_database();
is ($connerror, '', $t);
if ($connerror ne '') {
    BAIL_OUT 'Second connection to database failed, bailing out';
}

## Grab some important values used for the END output
my @vals = qw/array_nulls backslash_quote server_encoding client_encoding standard_conforming_strings/;
my $SQL = 'SELECT name,setting FROM pg_settings WHERE name IN (' .
    (join ',' => map { qq{'$_'} } @vals) . ')';
for (@{$dbh->selectall_arrayref($SQL)}) {
    my ($name, $value) = @$_;
    ## Skip 'normal' settings
    next if $name eq 'array_nulls' and $value eq 'on';
    next if $name eq 'standard_conforming_strings' and $value eq 'on';
    next if $name eq 'backslash_quote' and $value ne 'off';
    next if $name =~ /encoding/ and $value eq 'UTF8';
    $setting{$name} = $value;
}

my $dbh2 = connect_database();

pass ('Connect with second database handle');

my $sth = $dbh->prepare('SELECT 123');
ok ($dbh->disconnect(), 'Disconnect first database handle');
ok ($dbh2->disconnect(), 'Disconnect second database handle (first attempt)');
ok ($dbh2->disconnect(), 'Disconnect second database handle (second attempt)');

$t=q{Calling $sth->execute() fails on a disconnected statement};
eval { $sth->execute() };
ok ($@, $t);

## A failure to produce a valid arg for libpq will give a message like this:
## DBI connect('dbname=dbdpg_test;baldrick=0','',...) failed:
## invalid connection option "baldrick"

$t=q{Calling DBI->connect() fails with an invalid option};
my $bad_dsn = 'dbi:Pg:dbname=dbdpg_test;baldrick=0';
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, qr/DBI.*baldrick/, $t);

$t=q{Calling DBI->connect() works with database as "XXX"};
for my $opt (qw/db dbname database/) {
    $bad_dsn = "dbi:Pg:$opt=dbdpg_test;edmund=1";
    eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
    (my $tname = $t) =~ s/XXX/$opt/;
    like ($@, qr/DBI.*edmund/, $tname);
}

$t=q{Calling DBI->connect() works with forced uppercase 'DBI:'};
my ($testdsn,$testuser,undef,$su,$uid,$testdir,$pg_ctl,$initdb,$error,$version) ## no critic (Variables::ProhibitUnusedVarsStricter)
    = get_test_settings();
$testdsn =~ s/^dbi/DBI/i;
my $tempdbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS});
ok (ref $tempdbh, $t);
$tempdbh->disconnect();

$t=q{Calling DBI->connect() works with mixed case 'DbI:'}; ## nospellcheck
$testdsn =~ s/^dbi/DbI/i;
$tempdbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS});
ok (ref $tempdbh, $t);
$tempdbh->disconnect();

$t=q{Calling DBI->connect() fails with an improperly quoted dbname};
## A failure to produce a valid arg for libpq will give a message like this:
## failed: missing "=" after "s" in connection info string
$bad_dsn = q{dbi:Pg:dbname=dbdpg space name;port=1};
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, qr/=/, $t);

$t=q{Calling DBI->connect() fails with proper quoting but bad port};
## An otherwise correct call but to an invalid port gives a message like this:
## DBI connect('dbname='dbdpg \'spacey\' name';port=1','',...) failed:
## could not connect to server: No such file or directory
## Is the server running locally and accepting
## connections on Unix domain socket "/tmp/.s.PGSQL.1"?
$bad_dsn = q{dbi:Pg:dbname='dbdpg \'spacey\' name';port=1};
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, ($^O =~ /Win/ ? qr/DBI/s : qr/DBI.*\Q.s.PGSQL.1\E\b/s), $t);

 SKIP: {
     if ($pglibversion <= 100000) {
         skip ('Calling DBI->connect() with multiple host names requires libpq > 10', 1);
     }
     $t=q{Calling DBI->connect() works with multiple host names};
     (my $tempdsn = $testdsn) =~ s/host=/host=foo.invalid,/;
     $tempdbh = DBI->connect($tempdsn, $testuser, $ENV{DBI_PASS});
     ok (ref $tempdbh, $t);
     $tempdbh->do('select 1');
     $tempdbh->disconnect();
}


 SKIP: {
     my @names = ('foo', 'foo bar', ';foo;bar;', 'foo\'bar', 'foo\\\'bar', 'foo\';bar\';', '\\foo\\');
     if ($pgversion < 90000) {
         skip ('Calling DBI->connect() with an application_name requires Postgres >= 9.0', @names);
     }

     for my $aname (@names) {
         $t=qq{Calling DBI->connect() works with application name $aname};
         (my $escaped_name = $aname) =~ s/(['\\])/\\$1/g;
         $tempdbh = DBI->connect("$testdsn;application_name='$escaped_name'", $testuser, $ENV{DBI_PASS});
         if (! ref $tempdbh) {
             fail ("Failed to connect: $DBI::errstr");
             next;
         }
         my $returned_name = $tempdbh->selectrow_array('show application_name');
         is ($returned_name, $aname, $t);
         $tempdbh->disconnect;
     }
}

END {

    exit unless $verbose_end;

    my $perl_version = sprintf('%vd', $^V);
    my $schema = 'dbd_pg_testschema';
    my $dsn = exists $ENV{DBI_DSN} ? $ENV{DBI_DSN} : '?';

    ## Don't show current dir to the world via CPAN::Reporter results
    $dsn =~ s{host=/.*(testdb)}{host=<pwd>/$1};

    my $ver = defined $DBD::Pg::VERSION ? $DBD::Pg::VERSION : '?';
    my $user = exists $ENV{DBI_USER} ? $ENV{DBI_USER} : '<not set>';
    my $offset = 27;

    my $extra = '';

    ## Show interesting OS environment variables
    for my $name (sort qw/ LANG TZ /) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s %s", $offset, $name, $ENV{$name};
        }
    }

    ## Show interesting DBI environment variables
    for my $name (sort qw/
        DBI_DRIVER DBI_AUTOPROXY DBI_PASS DBI_DBNAME
        DBI_TRACE DBI_PROFILE DBI_PUREPERL PERL_DBI_DEBUG
     /) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s %s", $offset, $name,
                $name =~ /PASS/ ? '<not shown>' : $ENV{$name};
        }
    }

    ## Show interesting Postgres libpq environment variables
    for my $suffix (sort qw/
        HOST HOSTADDR PORT DATABASE USER PASSWORD PASSFILE
        REQUIREAUTH CHANNELBINDING SERVICE SERVICEFILE OPTIONS
        APPNAME SSLMODE REQUIRESSL KRBSRVNAME CONNECT_TIMEOUT
        DATESTYLE TZ
        SYSCONFDIR LOCALEDIR
     /) {
        my $name = "PG$suffix";
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s %s", $offset, $name,
                $name =~ /PASSWORD/ ? '<not shown>' : $ENV{$name};
        }
    }

    ## Show all DBDPG environment variables
    for my $name (grep { /^DBDPG/ } sort keys %ENV) {
        $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
    }

    ## Show interesting 'Postgres' environment variables
    for my $name (sort qw/ POSTGRES_INCLUDE POSTGRES_LIB POSTGRES_HOME /) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s %s", $offset, $name, $ENV{$name};
        }
    }

    ## Show Perl testing related environment variables
    for my $name (qw/ RELEASE_TESTING AUTHOR_TESTING TEST_SIGNATURE EXTENDED_TESTING AUTOMATED_TESTING /) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
        }
    }

    ## Show things of interest we grabbed from pg_settings
    for my $name (sort keys %setting) {
        $extra .= sprintf "\n%-*s %s", $offset, $name, $setting{$name};
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
        "Perl                        Version $perl_version\n".
        "OS                          $^O\n".
        "PostgreSQL (compiled)       $pglibversion\n".
        "PostgreSQL (target)         $pgversion\n".
        "PostgreSQL (reported)       $pgvstring\n".
        "Default port                $pgdefport\n".
        "DBI_DSN                     $dsn\n".
        "DBI_USER                    $user\n".
        "Test schema                 $schema$extra\n";

    if ($extra =~ /Error was/ and $extra !~ /probably not available/) {
        BAIL_OUT "Cannot continue: connection failed\n";
    }
}
