
## Helper file for the DBD::Pg tests

use strict;
use warnings;
use Data::Dumper;
use DBI;
use Cwd;
use 5.008001;
select(($|=1,select(STDERR),$|=1)[1]);

my $superuser = 1;

my $testfh;
if (exists $ENV{TEST_OUTPUT}) {
    my $file = $ENV{TEST_OUTPUT};
    open $testfh, '>>', $file or die qq{Could not append file "$file": $!\n};
    Test::More->builder->failure_output($testfh);
    Test::More->builder->todo_output($testfh);
}

my @matviews =
    (
     'dbd_pg_matview',
     );

my @operators =
    (
        '?.integer.integer',
        '??.text.text',
    );

my @schemas =
    (
     'dbd_pg_testschema',
     'dbd_pg_testschema2',
     );

my @tables =
    (
     'dbd_pg_test5',
     'dbd_pg_test4',
     'dbd_pg_test3',
     'dbd_pg_testschema2.dbd_pg_test3',
     'dbd_pg_testschema2.dbd_pg_test2',
     'dbd_pg_test2',
     'dbd_pg_test1',
     'dbd_pg_test',
     'dbd_pg_test_geom',
     );

my @sequences =
    (
     'dbd_pg_testsequence',
     'dbd_pg_testschema2.dbd_pg_testsequence2',
     'dbd_pg_testschema2.dbd_pg_testsequence3',
     );

## Schema used for testing:
my $S = 'dbd_pg_testschema';

## File written so we don't have to retry connections:
my $helpfile = 'README.testdatabase';

use vars qw/$fh/;

sub connect_database {

    ## Connect to the database (unless 'dbh' is passed in)
    ## Setup all the tables (unless 'nocreate' is passed in)
    ## Returns three values:
    ## 1. helpconnect for use by 01connect.t
    ## 2. Any error generated
    ## 3. The database handle, or undef

    my $arg = shift || {};
    ref $arg and ref $arg eq 'HASH' or die qq{Need a hashref!\n};

    my $dbh = $arg->{dbh} || '';
    my $alias = qr{(database|db|dbname)};
    my $info;
    my $olddir = getcwd;
    my $debug = $ENV{DBDPG_DEBUG} || 0;
    delete @ENV{ 'PGSERVICE', 'PGDATABASE' };


    ## We'll try various ways to get to a database to test with

    ## First, check to see if we've been here before and left directions
    my ($testdsn,$testuser,$helpconnect,$su,$uid,$testdir,$pg_ctl,$initdb,$error,$version)
        = get_test_settings();

    if ($debug) {
        diag "Test settings:
dsn: $testdsn
user: $testuser
helpconnect: $helpconnect
su: $su
uid: $uid
testdir: $testdir
pg_ctl: $pg_ctl
initdb: $initdb
error: $error
version: $version
";
        for my $key ( grep { /^DBDPG/ } keys %ENV ) {
            diag "ENV $key = $ENV{$key}\n";
        }
    }

    ## Did we fail last time? Fail this time too, but quicker!
    if ($testdsn =~ /FAIL!/) {
        $debug and diag 'Previous failure detected';
        return $helpconnect, "Previous failure ($error)", undef;
    }

    ## We may want to force an initdb call
    if ((!$helpconnect and $ENV{DBDPG_TESTINITDB})
            or (exists $ENV{DBDPG_INITDB} and $initdb ne $ENV{DBDPG_INITDB})) {
        $debug and diag 'Jumping to INITDB';
        goto INITDB;
    }

    ## Got a working DSN? Give it an attempt
    if ($testdsn and $testuser) {

        $debug and diag "Trying with $testuser and $testdsn";

        ## Used by t/01connect.t
        if ($arg->{dbreplace}) {
            $testdsn =~ s/$alias\s*=/$arg->{dbreplace}=/;
        }
        if ($arg->{dbquotes}) {
            $testdsn =~ s/$alias\s*=([\-\w]+)/'db="'.lc $2.'"'/e;
        }

        goto GOTDBH if eval {
            $dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
                                {RaiseError => 1, PrintError => 0, AutoCommit => 1});
            1;
        };

        $debug and diag "Connection failed: $@";

        if ($@ =~ /invalid connection option/ or $@ =~ /dbbarf/) {
            return $helpconnect, $@, undef;
        }

        if ($arg->{nocreate}) {
            return $helpconnect, '', undef;
        }

        ## If this was created by us, try and restart it
        if (16 == $helpconnect) {

            ## Bypass if the testdir has been removed
            if (! -e $testdir) {
                $arg->{nocreate} and return $helpconnect, '', undef;
                warn "Test directory $testdir has been removed, will create a new one\n";
            }
            else {
                if (-e "$testdir/data/postmaster.pid") {
                    ## Assume it's up, and move on
                }
                else {

                    if ($arg->{norestart}) {
                        return $helpconnect, '', undef;
                    }

                    warn "Restarting test database $testdsn at $testdir\n";
                    if ($^O !~ /Win32/) {
                        my $sockdir = "$testdir/data/socket";
                        if (! -e $sockdir) {
                            mkdir $sockdir;
                            if ($uid) {
                                if (! chown $uid, -1, $sockdir) {
                                    warn "chown of $sockdir failed!\n";
                                }
                            }
                        }
                    }
                    my $COM = qq{$pg_ctl -o '-k $testdir/data/socket' -l $testdir/dbdpg_test.logfile -D $testdir/data start};
                    if ($su) {
                        $COM = qq{su -m $su -c "$COM"};
                        chdir $testdir;
                    }
                    $info = '';
                    eval { $info = qx{$COM}; };
                    my $err = $@;
                    $su and chdir $olddir;
                    if ($err or $info !~ /\w/) {
                        $err = "Could not startup new database ($err) ($info)";
                        return $helpconnect, $err, undef;
                    }
                    ## Wait for it to startup and verify the connection
                    sleep 1;
                }
                my $loop = 1;
              STARTUP: {
                    eval {
                        $dbh = DBI->connect($testdsn, $testuser, '',
                                            {RaiseError => 1, PrintError => 0, AutoCommit => 1});
                    };
                    if ($@ =~ /starting up/ or $@ =~ /PGSQL\.\d+/) {
                        if ($loop++ < 20) {
                            sleep 1;
                            redo STARTUP;
                        }
                    }
                }

                if ($@) {
                    return $helpconnect, $@, $dbh;
                }

                ## We've got a good connection, so do final tweaks and return
                goto GOTDBH;

            } ## end testdir exists

        } ## end error and we created this database

    } ## end got testdsn and testuser

    ## No previous info (or failed attempt), so try to connect and possible create our own cluster

    $testdsn ||= $ENV{DBI_DSN};
    $testuser ||= $ENV{DBI_USER};

    if (! $testdsn) {
        $helpconnect = 1;
        $testdsn = $^O =~ /Win32/ ? 'dbi:Pg:host=localhost' : 'dbi:Pg:';
    }
    if (! $testuser) {
        $testuser = 'postgres';
    }

    ## From here on out, we don't return directly, but save it first
  GETHANDLE: {
        eval {
            $dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
                                {RaiseError => 1, PrintError => 0, AutoCommit => 1});
        };

        last GETHANDLE if ! $@; ## Made it!
        ## If the error was because of the user, try a few others
        if ($@ =~ /postgres/) {

            if ($helpconnect) {
                $testdsn .= ';dbname=postgres';
                $helpconnect += 2;
            }
            $helpconnect += 4;
            $testuser = $^O =~
                /openbsd/ ? '_postgresql'
                : $^O =~ /bsd/i ? 'pgsql'
                : 'postgres';
            eval {
                $dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
                                    {RaiseError => 1, PrintError => 0, AutoCommit => 1});
            };
            last GETHANDLE if ! $@; ## Made it!

            ## Final user tweak: set to postgres for Beastie
            if ($testuser ne 'postgres') {
                $helpconnect += 8;
                $testuser = 'postgres';
                eval {
                    $dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
                                        {RaiseError => 1, PrintError => 0, AutoCommit => 1});
                };
                last GETHANDLE if ! $@; ## Made it!
            }
        }

        ## Cannot connect to an existing database, so we'll create our own
        if ($arg->{nocreate}) {
            return $helpconnect, '', undef;
        }

      INITDB:
        my $testport;
        $helpconnect = 16;

        ## Let the ENV variables win
        for my $key (qw/ DBDPG_INITDB PGINITDB /) {
            if (exists $ENV{$key} and length $ENV{$key}) {
                $initdb = $ENV{$key};
                last;
            }
        }

        ## Use the initdb found by App::Info
        if (! length $initdb or $initdb eq 'default') {
            $initdb = 'initdb';
        }
        if ($initdb ne 'initdb' and ! -e $initdb) {
            die "Invalid initdb: $initdb\n";
        }

        ## Make sure initdb exists and is working properly
        $ENV{LANG} = 'C';
        $info = '';
        eval {
            $info = qx{$initdb --version 2>&1};
        };
        last GETHANDLE if $@; ## Fail - initdb bad
        $version = 0;
        if (!defined $info or ($info !~ /Postgres/i and $info !~ /run as root/)) {
            if (defined $info) {
                if ($info !~ /\w/) {
                    $@ = 'initdb not found: cannot run full tests without a Postgres database';
                }
                else {
                    $@ = "Bad initdb output: $info";
                }
            }
            else {
                my $msg = 'Failed to run initdb (executable probably not available)';
                exists $ENV{DBDPG_INITDB} and $msg .= " ENV was: $ENV{DBDPG_INITDB}";
                $msg .= " Final call was: $initdb";
                $@ = $msg;
            }
            last GETHANDLE; ## Fail - initdb bad
        }
        elsif ($info =~ /(\d+\.\d+)/) {
            $version = $1;
        }
        elsif ($info =~ /(\d+)(?:devel|beta|rc|alpha)/) { ## Can be 10devel
            $version = $1;
        }
        else {
            die "No version from initdb?! ($info)\n";
        }

        ## Make sure pg_ctl is available as well before we go further
        if (! -e $pg_ctl) {
            $pg_ctl = 'pg_ctl';
        }
        $info = '';
        eval {
            $info = qx{$pg_ctl --help 2>&1};
        };
        last GETHANDLE if $@; ## Fail - pg_ctl bad
        if (!defined $info or ($info !~ /\@[a-z.-]*?postgresql\.org/ and $info !~ /run as root/)) {
            $@ = defined $initdb ? "Bad pg_ctl output: $info" : 'Bad pg_ctl output';
            last GETHANDLE; ## Fail - pg_ctl bad
        }

        ## initdb and pg_ctl seems to be available, let's use them to fire up a cluster
        warn "Please wait, creating new database (version $version) for testing\n";
        $info = '';
        eval {
            my $com = "$initdb --locale=C -E UTF8 -D $testdir/data";
            $debug and warn" Attempting: $com\n";
            $info = qx{$com 2>&1};
        };
        last GETHANDLE if $@; ## Fail - initdb bad

        ## initdb and pg_ctl cannot be run as root, so let's handle that
        if ($info =~ /run as root/ or $info =~ /unprivilegierte/) {

            my $founduser = 0;
            $su = $testuser = '';

            ## Figure out a valid directory - returns empty if nothing available
            $testdir = find_tempdir();
            if (!$testdir) {
                return $helpconnect, 'Unable to create a temp directory', undef;
            }

            my $readme = "$testdir/README";
            if (open $fh, '>', $readme) {
                print $fh "This is a test directory for DBD::Pg and may be removed\n";
                print $fh "You may want to ensure the postmaster has been stopped first.\n";
                print $fh "Check the data/postmaster.pid file\n";
                close $fh or die qq{Could not close "$readme": $!\n};
            }

            ## Likely candidates for running this
            my @userlist = (qw/postgres postgresql pgsql _postgres/);

            ## Start with whoever owns this file, unless it's us
            my $username = getpwuid ((stat($0))[4]);
            unshift @userlist, $username if defined $username and $username ne getpwent;

            my %doneuser;
            for (@userlist) {
                $testuser = $_;
                next if $doneuser{$testuser}++;
                $uid = (getpwnam $testuser)[2];
                next if !defined $uid;

                next unless chown $uid, -1, $testdir;
                next unless chown $uid, -1, $readme;
                $su = $testuser;
                $founduser++;
                $info = '';
                $olddir = getcwd;
                eval {
                    chdir $testdir;
                    $info = qx{su -m $testuser -c "$initdb --locale=C -E UTF8 -D $testdir/data 2>&1"};
                };
                my $err = $@;
                chdir $olddir;
                last if !$err;
            }
            if (!$founduser) {
                $@ = 'Unable to find a user to run initdb as';
                last GETHANDLE; ## Fail - no user
            }
            if (! -e "$testdir/data") {
                $@ = 'Could not create a test database via initdb';
                last GETHANDLE; ## Fail - no datadir created
            }
            ## At this point, both $su and $testuser are set
        }

        if ($info =~ /FATAL/) {
            $@ = "initdb gave a FATAL error: $info";
            last GETHANDLE; ## Fail - FATAL
        }

        if ($info =~ /but is not empty/) {
            ## Assume this is already good to go
        }
        elsif ($info !~ /pg_ctl/) {
            $@ = "initdb did not give a pg_ctl string: $info";
            last GETHANDLE; ## Fail - bad output
        }

        ## Which user do we connect as?
        if (!$su and $info =~ /owned by user "(.+?)"/) {
            $testuser = $1;
        }

        ## Attempt to boost the system oids above an int for certain testing
        (my $resetxlog = $initdb) =~ s/initdb/pg_resetxlog/;
        if ($version >= 10) {
            $resetxlog =~ s/pg_resetxlog/pg_resetwal/;
        }
        eval {
            $info = qx{$resetxlog --help};
        };
        if (! $@ and $info =~ /XID/) {
            if (! -e "$testdir/data/postmaster.pid") {
                eval {
                    $info = qx{ $resetxlog -o 2222333344 $testdir/data };
                };
                ## We don't really care if it worked or not!
            }
        }

        ## Now we need to find an open port to use
        $testport = 5442;
        ## If we've got netstat available, we'll trust that
        $info = '';
        my $evalok = 0;
        eval {
            $info = qx{netstat -na 2>&1};
            $evalok = 1;
        };
        if (!$evalok or ! defined $info) {
            warn "netstat call failed, trying port $testport\n";
        }
        else {
            ## Start at 5440 and go up until we are free
            $testport = 5440;
            my $maxport = 5470;
            {
                last if $info !~ /PGSQL\.$testport$/m
                    and $info !~ /\b127\.0\.0\.1:$testport\b/m;
                last if ++$testport >= $maxport;
                redo;
            }
            if ($testport >= $maxport) {
                $@ = "No free ports found for testing: tried 5440 to $maxport\n";
                last GETHANDLE; ## Fail - no free ports
            }
        }
        $@ = '';

        $debug and diag "Port to use: $testport";

        my $conf = "$testdir/data/postgresql.conf";
        my $cfh;

        ## If there is already a pid file, do not modify the config
        ## We assume a previous run put it there, so we extract the port
        if (-e "$testdir/data/postmaster.pid") {
            $debug and diag qq{File "$testdir/data/postmaster.pid" exists};
            open my $cfh, '<', $conf or die qq{Could not open "$conf": $!\n};
            while (<$cfh>) {
                if (/^\s*port\s*=\s*(\d+)/) {
                    $testport = $1;
                    $debug and diag qq{Found port $testport inside conf file\n};
                }
            }
            close $cfh or die qq{Could not close "$conf": $!\n};
            ## Assume it's up, and move on
        }
        else {
            ## Change to this new port and fire it up
            if (! open $cfh, '>>', $conf) {
                $@ = qq{Could not open "$conf": $!};
                $debug and diag qq{Failed to open "$conf"};
                last GETHANDLE; ## Fail - no conf file
            }
            $debug and diag qq{Writing to "$conf"};
            print $cfh "\n\n## DBD::Pg testing parameters\n";
            print $cfh "port=$testport\n";
            print $cfh "max_connections=11\n";
            print $cfh "log_statement = 'all'\n";
            print $cfh "log_line_prefix = '%m [%p] '\n";
            print $cfh "log_filename = 'postgres%Y-%m-%d.log'\n";
            print $cfh "log_rotation_size = 0\n";
            if (8.1 == $version) {
                print {$cfh} "redirect_stderr = on\n";
            }

            if ($version >= 8.3) {
                print {$cfh} "logging_collector = on\n";
            }
            print $cfh "log_min_messages = 'DEBUG1'\n";

            if ($version >= 9.4) {
                print $cfh "wal_level = logical\n";
                print $cfh "max_replication_slots = 1\n";
                print $cfh "max_wal_senders = 1\n";

                open my $hba, '>>', "$testdir/data/pg_hba.conf"
                    or die qq{Could not open "$testdir/data/pg_hba.conf": $!\n};

                print $hba "local\treplication\tall\ttrust\n";
                print $hba "host\treplication\tall\t127.0.0.1/32\ttrust\n";
                print $hba "host\treplication\tall\t::1/128\ttrust\n";

                close $hba or die qq{Could not close "$testdir/data/pg_hba.conf": $!\n};
            }

            print $cfh "listen_addresses='127.0.0.1'\n" if $^O =~ /Win32/;
            print $cfh "\n";
            close $cfh or die qq{Could not close "$conf": $!\n};

            ## Attempt to start up the test server
            $info = '';
            if ($^O !~ /Win32/) {
                my $sockdir = "$testdir/data/socket";
                if (! -e $sockdir) {
                    mkdir $sockdir;
                    if ($su) {
                        if (! chown $uid, -1, $sockdir) {
                            warn "chown of $sockdir failed!\n";
                        }
                    }
                }
            }
            my $COM = qq{$pg_ctl -o '-k $testdir/data/socket' -l $testdir/dbdpg_test.logfile -D $testdir/data start};
            $olddir = getcwd;
            if ($su) {
                chdir $testdir;
                $COM = qq{su -m $su -c "$COM"};
            }
            $debug and diag qq{Running: $COM};
            eval {
                $info = qx{$COM};
            };
            my $err = $@;
            $su and chdir $olddir;
            if ($err or $info !~ /\w/) {
                $@ = "Could not startup new database ($COM) ($err) ($info)";
                last GETHANDLE; ## Fail - startup failed
            }
            sleep 1;
        }

        ## Attempt to connect to this server
        $testdsn = "dbi:Pg:dbname=postgres;port=$testport";
        if ($^O =~ /Win32/) {
            $testdsn .= ';host=localhost';
        }
        else {
            $testdsn .= ";host=$testdir/data/socket";
        }

        $debug and diag qq{Test DSN: $testdsn};
        my $loop = 1;
      STARTUP: {
            eval {
                $dbh = DBI->connect($testdsn, $testuser, '',
                                    {RaiseError => 1, PrintError => 0, AutoCommit => 1});
            };
            ## Regardless of the error, try again.
            ## We used to check the message, but LANG problems may complicate that.
            if ($@) {

                $debug and diag qq{Connection error: $@\n};

                if ($@ =~ /database "postgres" does not exist/) {
                    ## Old server, so let's create a postgres database manually
                    sleep 2;
                    (my $tempdsn = $testdsn) =~ s/postgres/template1/;
                    eval {
                        $dbh = DBI->connect($tempdsn, $testuser, '',
                                            {RaiseError => 1, PrintError => 0, AutoCommit => 1});
                    };
                    if ($@) {
                        die "Could not connect: $@\n";
                    }
                    $dbh->do('CREATE DATABASE postgres');
                    $dbh->disconnect();
                }

                if ($@ =~ /role "postgres" does not exist/) {
                    ## Probably just created with the current user, so use that
                    if (exists $ENV{USER} and length $ENV{USER}) {
                        $debug and diag qq{Switched to new user: $testuser\n};
                        eval {
                            $dbh = DBI->connect($testdsn, $ENV{USER}, '',
                                                {RaiseError => 1, PrintError => 0, AutoCommit => 1});
                        };
                        if ($@) {
                            die "Could not connect: $@\n";
                        }
                        $dbh->do('CREATE USER postgres SUPERUSER');
                        $dbh->disconnect();
                    }
                }

                if ($loop++ < 5) {
                    sleep 1;
                    redo STARTUP;
                }
            }
            last GETHANDLE; ## Made it!
        }

    } ## end of GETHANDLE

    ## At this point, we've got a connection, or have failed
    ## Either way, we record for future runs

    my $connerror = $@;
    if (open $fh, '>', $helpfile) {
        print $fh "## This is a temporary file created for testing DBD::Pg\n";
        print $fh '## Created: ' . scalar localtime() . "\n";
        print $fh "## Feel free to remove it!\n";
        print $fh "## Helpconnect: $helpconnect\n";
        print $fh "## pg_ctl: $pg_ctl\n";
        print $fh "## initdb: $initdb\n";
        print $fh "## Version: $version\n";
        if ($connerror) {
            print $fh "## DSN: FAIL!\n";
            print $fh "## ERROR: $connerror\n";
        }
        else {
            print $fh "## DSN: $testdsn\n";
            print $fh "## User: $testuser\n";
            print $fh "## Testdir: $testdir\n" if 16 == $helpconnect;
            print $fh "## Testowner: $su\n" if $su;
            print $fh "## Testowneruid: $uid\n" if $uid;
        }
        close $fh or die qq{Could not close "$helpfile": $!\n};
    }

    $connerror and return $helpconnect, $connerror, undef;

  GOTDBH:
    ## This allows things like data_sources() to work if we did an initdb
    $ENV{DBI_DSN} = $testdsn;
    $ENV{DBI_USER} = $testuser;

    $debug and diag "Got a database handle ($dbh)";

    if (!$arg->{quickreturn} or 1 != $arg->{quickreturn}) {
        ## non-ASCII parts of the tests assume UTF8
        $dbh->do('SET client_encoding = utf8');
        $dbh->{pg_enable_utf8} = -1;
    }

    if ($arg->{quickreturn}) {
        $debug and diag 'Returning via quickreturn';
        return $helpconnect, '', $dbh;
    }

    my $SQL = 'SELECT usesuper FROM pg_user WHERE usename = current_user';
    $superuser = $dbh->selectall_arrayref($SQL)->[0][0];
    if ($superuser) {
        $dbh->do(q{SET LC_MESSAGES = 'C'});
    }

    if ($arg->{nosetup}) {
        return $helpconnect, '', $dbh unless schema_exists($dbh, $S);
        $dbh->do("SET search_path TO $S");
    }
    else {

        $debug and diag 'Attempting to cleanup database';
        cleanup_database($dbh);

        eval {
            $dbh->do("CREATE SCHEMA $S");
        };
        $@ and $debug and diag "Create schema error: $@";
        if ($@ =~ /Permission denied/ and $helpconnect != 16) {
            ## Okay, this ain't gonna work, let's try initdb
            goto INITDB;
        }
        $@ and return $helpconnect, $@, undef;
        $dbh->do("SET search_path TO $S");
        eval { $dbh->do('CREATE SEQUENCE dbd_pg_testsequence'); };
        $@ and Test::More::BAIL_OUT('Failed to create test sequence');

        # If you add columns to this, please do not use reserved words!
        $SQL = q{
CREATE TABLE dbd_pg_test (
  id         integer not null primary key,
  lii        integer unique not null default nextval('dbd_pg_testsequence'),
  pname      varchar(20) default 'Testing Default' ,
  val        text,
  score      float CHECK(score IN ('1','2','3')),
  Fixed      character(5),
  pdate      timestamp default now(),
  testarray  text[][],
  testarray2 int[],
  testarray3 bool[],
  "CaseTest" boolean,
  expo       numeric(6,2),
  bytetest   bytea,
  bytearray  bytea[]
)
};

        $dbh->{Warn} = 0;
        eval { $dbh->do($SQL); };
        $@ and Test::More::BAIL_OUT('Failed to create test sequence');
        $dbh->{Warn} = 1;
        $dbh->do(q{COMMENT ON COLUMN dbd_pg_test.id IS 'Bob is your uncle'});

    } ## end setup

$dbh->commit() unless $dbh->{AutoCommit};

if ($arg->{disconnect}) {
    $dbh->disconnect();
    return $helpconnect, '', undef;
}

$dbh->{AutoCommit} = 0 unless $arg->{AutoCommit};
return $helpconnect, '', $dbh;

} ## end of connect_database


sub is_super {

    return $superuser;

}

sub find_tempdir {

    if (eval { require File::Temp; 1; }) {
        return File::Temp::tempdir('dbdpg_testdatabase_XXXXXX', TMPDIR => 1, CLEANUP => 0);
    }

    ## Who doesn't have File::Temp?! :)
    my $found = 0;
    for my $num (1..100) {
        my $tempdir = "/tmp/dbdpg_testdatabase_ABCDEF$num";
        next if -e $tempdir;
        mkdir $tempdir or return '';
        return $tempdir;
    }
    return '';

} ## end of find_tempdir


sub get_test_settings {

    ## Returns test database information from the testfile if it exists
    ## Defaults to ENV variables or blank

    ## Find the best candidate for the pg_ctl program
    my $pg_ctl = 'pg_ctl';
    my $initdb = 'default';
    if (exists $ENV{POSTGRES_HOME} and -e "$ENV{POSTGRES_HOME}/bin/pg_ctl") {
        $pg_ctl = "$ENV{POSTGRES_HOME}/bin/pg_ctl";
        $initdb = "$ENV{POSTGRES_HOME}/bin/initdb";
    }
    elsif (exists $ENV{DBDPG_INITDB} and -e $ENV{DBDPG_INITDB}) {
        ($pg_ctl = $ENV{DBDPG_INITDB}) =~ s/initdb/pg_ctl/;
    }
    elsif (exists $ENV{PGINITDB} and -e $ENV{PGINITDB}) {
        ($pg_ctl = $ENV{PGINITDB}) =~ s/initdb/pg_ctl/;
    }

    my ($testdsn, $testuser, $testdir, $error) = ('','','','?');
    my ($helpconnect, $su, $uid, $version) = (0,'','',0);
    my $inerror = 0;
    if (-e $helpfile) {
        open $fh, '<', $helpfile or die qq{Could not open "$helpfile": $!\n};
        while (<$fh>) {
            if ($inerror) {
                $error .= "\n$_";
            }
            /DSN: (.+)/           and $testdsn = $1;
            /User: (\S+)/         and $testuser = $1;
            /Helpconnect: (\d+)/  and $helpconnect = $1;
            /Testowner: (\w+)/    and $su = $1;
            /Testowneruid: (\d+)/ and $uid = $1;
            /Testdir: (.+)/       and $testdir = $1;
            /pg_ctl: (.+)/        and $pg_ctl = $1;
            /initdb: (.+)/        and $initdb = $1;
            /ERROR: (.+)/         and $error = $1 and $inerror = 1;
            /Version: (.+)/       and $version = $1;
        }
        close $fh or die qq{Could not close "$helpfile": $!\n};
    }

    if (!$testdir) {
        my $dir = getcwd();
        $testdir = "$dir/dbdpg_test_database";
    }

    ## Allow forcing of ENV variables
    if ($ENV{DBDPG_TEST_ALWAYS_ENV}) {
        $testdsn = $ENV{DBI_DSN} || '';
        $testuser = $ENV{DBI_USER} || '';
    }

    return $testdsn, $testuser, $helpconnect, $su, $uid, $testdir, $pg_ctl, $initdb, $error, $version;

} ## end of get_test_settings


sub schema_exists {

    my ($dbh,$schema) = @_;
    my $SQL = 'SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?';
    my $sth = $dbh->prepare_cached($SQL);
    my $count = $sth->execute($schema);
    $sth->finish();
    return $count < 1 ? 0 : 1;

} ## end of schema_exists


sub relation_exists {

    my ($dbh,$schema,$name) = @_;
    my $SQL = 'SELECT 1 FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n '.
        'WHERE n.oid=c.relnamespace AND n.nspname = ? AND c.relname = ?';
    my $sth = $dbh->prepare_cached($SQL);
    my $count = $sth->execute($schema,$name);
    $sth->finish();
    return $count < 1 ? 0 : 1;

} ## end of relation_exists


sub operator_exists {

    my ($dbh,$opname,$leftarg,$rightarg) = @_;

    my $schema = 'dbd_pg_testschema';
    my $SQL = 'SELECT 1 FROM pg_operator o, pg_namespace n '.
        'WHERE oprname=? AND oprleft = ?::regtype AND oprright = ?::regtype'.
            ' AND o.oprnamespace = n.oid AND n.nspname = ?';
    my $sth = $dbh->prepare_cached($SQL);
    my $count = $sth->execute($opname,$leftarg,$rightarg,$schema);
    $sth->finish();
    return $count < 1 ? 0 : 1;

} ## end of operator_exists


sub cleanup_database {

    ## Clear out any testing objects in the current database

    my $dbh = shift;
    my $type = shift || 0;

    return unless defined $dbh and ref $dbh and $dbh->ping();

    ## For now, we always run and disregard the type

    $dbh->rollback() if ! $dbh->{AutoCommit};

    for my $name (@matviews) {
        my $schema = ($name =~ s/(.+)\.(.+)/$2/) ? $1 : $S;
        next if ! relation_exists($dbh,$schema,$name);
        $dbh->do("DROP MATERIALIZED VIEW $schema.$name");
    }

    for my $name (@operators) {
        my ($opname,$leftarg,$rightarg) = split /\./ => $name;
        next if ! operator_exists($dbh,$opname,$leftarg,$rightarg);
        $dbh->do("DROP OPERATOR dbd_pg_testschema.$opname($leftarg,$rightarg)");
    }

    for my $name (@tables) {
        my $schema = ($name =~ s/(.+)\.(.+)/$2/) ? $1 : $S;
        next if ! relation_exists($dbh,$schema,$name);
        $dbh->do("DROP TABLE $schema.$name");
    }

    for my $name (@sequences) {
        my $schema = ($name =~ s/(.+)\.(.+)/$2/) ? $1 : $S;
        next if ! relation_exists($dbh,$schema,$name);
        $dbh->do("DROP SEQUENCE $schema.$name");
    }

    for my $schema (@schemas) {
        next if ! schema_exists($dbh,$schema);
        $dbh->do("DROP SCHEMA $schema CASCADE");
    }
    $dbh->commit() if ! $dbh->{AutoCommit};

    return;

} ## end of cleanup_database


sub shutdown_test_database {

    my ($testdsn,$testuser,$helpconnect,$su,$uid,$testdir,$pg_ctl,$initdb) = get_test_settings();

    if (-e $testdir and -e "$testdir/data/postmaster.pid") {
        my $COM = qq{$pg_ctl -D $testdir/data -m fast stop};
        my $olddir = getcwd;
        if ($su) {
            $COM = qq{su $su -m -c "$COM"};
            chdir $testdir;
        }
        eval {
            qx{$COM};
        };
        $su and chdir $olddir;
    }

    ## Remove the test directory entirely
    return if $ENV{DBDPG_TESTINITDB};
    return if ! eval { require File::Path; 1; };
    File::Path::rmtree($testdir);
    return;

} ## end of shutdown_test_database

1;
