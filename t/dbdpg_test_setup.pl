
## Helper file for the DBD::Pg tests

use strict;
use warnings;
use Data::Dumper;
use DBI;
use Cwd;
use 5.006;
select(($|=1,select(STDERR),$|=1)[1]);

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
	## Setup all the tables (unless 'nosetup' is passed in)
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

	## We'll try various ways to get to a database to test with

	## First, check to see if we've been here before and left directions
	my ($testdsn,$testuser,$helpconnect,$su,$uid,$testdir,$pg_ctl,$initdb,$error) = get_test_settings();

	## Did we fail last time? Fail this time too, but quicker!
	if ($testdsn =~ /FAIL!/) {
		return $helpconnect, "Previous failure ($error)", undef;
	}

	## We may want to force an initdb call
	if (!$helpconnect and $ENV{DBDPG_TESTINITDB}) {
		goto INITDB;
	}

	## Got a working DSN? Give it an attempt
	if ($testdsn and $testuser) {

		## Used by t/01connect.t
		if ($arg->{dbreplace}) {
			$testdsn =~ s/$alias\s*=/$arg->{dbreplace}=/;
		}
		if ($arg->{dbquotes}) {
			$testdsn =~ s/$alias\s*=([\-\w]+)/'db="'.lc $2.'"'/e;
		}

		goto GOTDBH if eval {
			$dbh = DBI->connect($testdsn, $testuser, '',
								{RaiseError => 1, PrintError => 0, AutoCommit => 1});
			1;
		};

		if ($@ =~ /invalid connection option/) {
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
					my $option = '';
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
						$option = q{-o '-k socket'};
					}
					my $COM = qq{$pg_ctl $option -l $testdir/dbdpg_test.logfile -D $testdir/data start};
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
				$testdsn .= 'dbname=postgres';
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

		## Use the initdb found by App::Info
		$initdb = $ENV{PGINITDB} || '';
		if (!$initdb or ! -e $initdb) {
			$initdb = 'initdb';
		}

		## Make sure initdb exists and is working properly
		$ENV{LANG} = 'C';
		$info = '';
		eval {
			$info = qx{$initdb --help 2>&1};
		};
		last GETHANDLE if $@; ## Fail - initdb bad
		if (!defined $info or ($info !~ /\@postgresql\.org/ and $info !~ /run as root/)) {
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
				exists $ENV{PGINITDB} and $msg .= " ENV was: $ENV{PGINITDB}";
				$msg .= " Final call was: $initdb";
				$@ = $msg;
			}
			last GETHANDLE; ## Fail - initdb bad
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
		if (!defined $info or ($info !~ /\@postgresql\.org/ and $info !~ /run as root/)) {
			$@ = defined $initdb ? "Bad pg_ctl output: $info" : 'Bad pg_ctl output';
			last GETHANDLE; ## Fail - pg_ctl bad
		}

		## initdb and pg_ctl seems to be available, let's use them to fire up a cluster
		warn "Please wait, creating new database for testing\n";
		$info = '';
		eval {
			$info = qx{$initdb --locale=C -E UTF8 -D $testdir/data 2>&1};
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

		## Now we need to find an open port to use
		$testport = 5442;
		## If we've got netstat available, we'll trust that
		$info = '';
		eval {
			$info = qx{netstat -na 2>&1};
		};
		if ($@) {
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
		## Change to this new port and fire it up
		my $conf = "$testdir/data/postgresql.conf";
		my $cfh;
		if (! open $cfh, '>>', $conf) {
			$@ = qq{Could not open "$conf": $!};
			last GETHANDLE; ## Fail - no conf file
		}
		print $cfh "\n\n## DBD::Pg testing parameters\n";
		print $cfh "port=$testport\n";
		print $cfh "max_connections=4\n";
		print $cfh "log_statement = 'all'\n";
		print $cfh "log_line_prefix = '%m [%p] '\n";
		print $cfh "log_min_messages = 'DEBUG1'\n";
		print $cfh "listen_addresses='127.0.0.1'\n" if $^O =~ /Win32/;
		print $cfh "\n";
		close $cfh or die qq{Could not close "$conf": $!\n};

		## Attempt to start up the test server
		if (-e "$testdir/data/postmaster.pid") {
			## Assume it's up, and move on
		}
		else {
			$info = '';
			my $option = '';
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
				$option = q{-o '-k socket'};
			}
			my $COM = qq{$pg_ctl $option -l $testdir/dbdpg_test.logfile -D $testdir/data start};
		    $olddir = getcwd;
			if ($su) {
				chdir $testdir;
				$COM = qq{su -m $su -c "$COM"};
			}
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
		my $loop = 1;
	  STARTUP: {
			eval {
				$dbh = DBI->connect($testdsn, $testuser, '',
									{RaiseError => 1, PrintError => 0, AutoCommit => 1});
			};
			## Regardless of the error, try again.
			## We used to check the message, but LANG problems may complicate that.
			if ($@) {
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

	if ($arg->{quickreturn}) {
		return $helpconnect, '', $dbh;
	}

	my $SQL = 'SELECT usesuper FROM pg_user WHERE usename = current_user';
	my $bga = $dbh->selectall_arrayref($SQL)->[0][0];
	if ($bga) {
		$dbh->do(q{SET LC_MESSAGES = 'C'});
	}

	if ($arg->{nosetup}) {
		return $helpconnect, '', $dbh unless schema_exists($dbh, $S);
		$dbh->do("SET search_path TO $S");
	}
	else {

		cleanup_database($dbh);

		eval {
			$dbh->do("CREATE SCHEMA $S");
		};
		if ($@ =~ /Permission denied/ and $helpconnect != 16) {
			## Okay, this ain't gonna work, let's try initdb
			goto INITDB;
		}
		$@ and return $helpconnect, $@, undef;
		$dbh->do("SET search_path TO $S");
		$dbh->do('CREATE SEQUENCE dbd_pg_testsequence');
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
  bytetest   bytea
)
};

		$dbh->{Warn} = 0;
		$dbh->do($SQL);
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

	## Returns test databae information from the testfile if it exists
	## Defaults to ENV variables or blank

	## Find the best candidate for the pg_ctl program
	my $pg_ctl = 'pg_ctl';
	if (exists $ENV{PGINITDB} and -e $ENV{PGINITDB}) {
		($pg_ctl = $ENV{PGINITDB}) =~ s/initdb/pg_ctl/;
	}
	my ($testdsn, $testuser, $testdir, $error) = ('','','','?');
	my ($helpconnect, $su, $uid, $initdb) = (0,'','','default');
	my $inerror = 0;
	if (-e $helpfile) {
		open $fh, '<', $helpfile or die qq{Could not open "$helpfile": $!\n};
		while (<$fh>) {
			if ($inerror) {
				$error .= "\n$_";
			}
			/DSN: (.+)/           and $testdsn = $1;
			/User: (\w+)/         and $testuser = $1;
			/Helpconnect: (\d+)/  and $helpconnect = $1;
			/Testowner: (\w+)/    and $su = $1;
			/Testowneruid: (\d+)/ and $uid = $1;
			/Testdir: (.+)/       and $testdir = $1;
			/pg_ctl: (.+)/        and $pg_ctl = $1;
			/initdb: (.+)/        and $initdb = $1;
			/ERROR: (.+)/         and $error = $1 and $inerror = 1;
		}
		close $fh or die qq{Could not close "$helpfile": $!\n};
	}

	if (!$testdir) {
		my $dir = getcwd();
		$testdir = "$dir/dbdpg_test_database";
	}

	return $testdsn, $testuser, $helpconnect, $su, $uid, $testdir, $pg_ctl, $initdb, $error;

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


sub cleanup_database {

	## Clear out any testing objects in the current database

	my $dbh = shift;
	my $type = shift || 0;

	return unless defined $dbh and ref $dbh and $dbh->ping();

	## For now, we always run and disregard the type

	$dbh->rollback() if ! $dbh->{AutoCommit};

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
		my $COM = qq{$pg_ctl -D $testdir/data --silent -m fast stop};
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
	warn "Removing test database directory\n";
	File::Path::rmtree($testdir);
	return;

} ## end of shutdown_test_database

1;
