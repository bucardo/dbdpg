#!perl

## Make sure we can connect and disconnect cleanly
## All tests are stopped if we cannot make the first connect

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use DBI;
use DBD::Pg;
use Test::More;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

## Define this here in case we get to the END block before a connection is made.
our ($t, $pgversion, $pglibversion, $pgvstring, $pgdefport, $helpconnect, $dbh, $connerror, %setting);
BEGIN {
    ($pgversion,$pglibversion,$pgvstring,$pgdefport) = ('?','?','?','?');
}

($helpconnect,$connerror,$dbh) = connect_database();

if (! defined $dbh or $connerror) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

pass ('Connection to test database works');

$pgversion    = $dbh->{pg_server_version};
$pglibversion = $dbh->{pg_lib_version};
$pgdefport    = $dbh->{pg_default_port};
$pgvstring    = $dbh->selectall_arrayref('SELECT VERSION()')->[0][0];

ok ($dbh->disconnect(), 'Calling $dbh->disconnect() works');

# Connect two times. From this point onward, do a simpler connection check
$t=q{Second database connection works};
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

$t=q{Calling execute() fails on a disconnected statement};
eval { $sth->execute() };
ok ($@, $t);

## A failure to produce a valid arg for libpq will give a message like this:
## DBI connect('dbname=dbdpg_test;baldrick=0','',...) failed: 
## invalid connection option "baldrick"

$t=q{Calling DBI->connect() with an invalid option fails};
my $bad_dsn = 'dbi:Pg:dbname=dbdpg_test;baldrick=0';
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, qr/DBI.*"baldrick"/, $t);

$t=q{Calling DBI->connect with database as "XXX" works};
for my $opt (qw/db dbname database/) {
    $bad_dsn = "dbi:Pg:$opt=dbdpg_test;edmund=1";
    eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
    (my $tt = $t) =~ s/XXX/$opt/;
    like ($@, qr/DBI.*"edmund"/, $tt);
}

$t=q{Calling DBI->connect() with forced uppercase 'DBI:' works};
my ($testdsn,$testuser,undef,$su,$uid,$testdir,$pg_ctl,$initdb,$error,$version) ## no critic (Variables::ProhibitUnusedVarsStricter)
    = get_test_settings();
$testdsn =~ s/^dbi/DBI/i;
my $ldbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS});
ok (ref $ldbh, $t);
$ldbh->disconnect();

$t=q{Calling DBI->connect() with mixed case 'DbI:' works}; ## nospellcheck
$testdsn =~ s/^dbi/DbI/i;
$ldbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS});
ok (ref $ldbh, $t);
$ldbh->disconnect();

$t=q{Calling DBI->connect() with an improperly quoted dbname fails};
## A failure to produce a valid arg for libpq will give a message like this:
## failed: missing "=" after "s" in connection info string
$bad_dsn = q{dbi:Pg:dbname=dbdpg space name;port=1};
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, qr/"="/, $t);

$t=q{Calling DBI->connect() with proper quoting but bad port gives expected error};
## An otherwise correct call but to an invalid port gives a message like this:
## DBI connect('dbname='dbdpg \'spacey\' name';port=1','',...) failed: 
## could not connect to server: No such file or directory
## Is the server running locally and accepting
## connections on Unix domain socket "/tmp/.s.PGSQL.1"?
$bad_dsn = q{dbi:Pg:dbname='dbdpg \'spacey\' name';port=1};
eval { DBI->connect($bad_dsn, '', '', {RaiseError=>1}) };
like ($@, qr/DBI.*\Q.s.PGSQL.1\E\b/s, $t);

 SKIP: {
     if ($pglibversion <  100000) {
         skip ('Multiple host names requires libpq >= 10', 1);
     }
     $t=q{Calling DBI->connect() with multiple host names works};
     (my $tempdsn = $testdsn) =~ s/host=/host=foo.invalid,/;
     $ldbh = DBI->connect($tempdsn, $testuser, $ENV{DBI_PASS});
     ok (ref $ldbh, $t);
     $ldbh->do('select 1');
     $ldbh->disconnect();
}


 SKIP: {
     my @names = ('foo', 'foo bar', ';foo;bar;', 'foo\'bar', 'foo\\\'bar', 'foo\';bar\';', '\\foo\\');
     if ($pgversion < 90000) {
         skip ('Calling DBI->connect() with an application_name requires Postgres >= 9.0', @names * 2);
     }

     for my $aname (@names) {
         $t=qq{Calling DBI->connect() with aname=$aname};
         (my $escaped_name = $aname) =~ s/(['\\])/\\$1/g;
         my $adbh = DBI->connect("$testdsn;application_name='$escaped_name'", $testuser, $ENV{DBI_PASS});
         if (! ref $adbh) {
             fail ("Failed to connect: $DBI::errstr");
             next;
         }
         my $returned_name = $adbh->selectrow_array('show application_name');
         $t=qq{Setting application_name on connect() returns correct value for: $aname};
         is ($returned_name, $aname, $t);
         $adbh->disconnect;
     }
}

done_testing();

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
        if (exists $ENV{$name} and defined $ENV{$name} and $ENV{$name}) {
            $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
        }
    }

    for my $name (grep { /^DBDPG/ } sort keys %ENV) {
        $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
    }

    for my $name (qw/ RELEASE_TESTING AUTHOR_TESTING /) {
        if (exists $ENV{$name} and defined $ENV{$name}) {
            $extra .= sprintf "\n%-*s $ENV{$name}", $offset, $name;
        }
    }

    ## More helpful stuff
    for (sort keys %setting) {
        $extra .= sprintf "\n%-*s %s", $offset, $_, $setting{$_};
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

    if ($extra =~ /Error was/ and $extra !~ /probably not available/) {
        BAIL_OUT "Cannot continue: connection failed\n";
    }
}
