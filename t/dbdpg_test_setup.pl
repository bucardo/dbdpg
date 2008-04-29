
## Helper file for the DBD::Pg tests

use strict;
use warnings;
use Data::Dumper;
use DBI;
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

## If we create our own cluster, store it here:
my $test_database_dir = 'dbdpg_test_database';


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

	## We'll try various ways to get to a database to test with

	## First, check to see if we've been here before and left directions
	my ($testdsn,$testuser,$helpconnect,$su,$testdir) = get_test_settings();

	## Did we fail last time? Fail this time too, but quicker!
	if ($testdsn =~ /FAIL!/) {
		return $helpconnect, 'Previous failure', undef;
	}

	## Got a working DSN? Give it an attempt
	if ($testdsn and $testuser) {

		## Used by t/01connect.t
		if ($arg->{dbreplace}) {
			$testdsn =~ s/$alias\s*=/$arg->{dbreplace}=/;
		}
		if ($arg->{dbquotes}) {
			$testdsn =~ s/$alias\s*=(\w+)/'db="'.lc $2.'"'/e;
		}
		eval {
			$dbh = DBI->connect($testdsn, $testuser, '',
								{RaiseError => 1, PrintError => 0, AutoCommit => 1});
		};
		if ($@) {
			if ($@ !~ /domain socket/ or 16 != $helpconnect) {
				return $helpconnect, $@, undef;
			}

			## If we created it, and it was shut down, start it up again
			warn "Restarting test database $testdsn at $testdir\n";

			my $COM = qq{pg_ctl -l $testdir/dbdpg_test.logfile -D $testdir start};
			if ($su) {
				$COM = qq{su -l $su -c "$COM"};
			}
			$info = '';
			eval { $info = qx{$COM}; };
			if ($@ or $info !~ /\w/) {
				$@ = "Could not startup new database ($@) ($info)";
				return $helpconnect, $@, undef;
			}
			## Wait for it to startup and verify the connection
			sleep 1;
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

		} ## end got an error on connect attempt

		## We've got a good connection, so do final tweaks and return
		goto GOTDBH;

	} ## end got testdsn and testuser

	## No previous info, so start connection attempt from scratch

	$testdsn ||= $ENV{DBI_DSN};
	$testuser ||= $ENV{DBI_USER};

	if (! $testdsn) {
		$helpconnect = 1;
		$testdsn = 'dbi:Pg:';
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
		last GETHANDLE if ! $@;

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
			last GETHANDLE if ! $@;

			## Final user tweak: set to postgres for Beastie
			if ($testuser ne 'postgres') {
				$helpconnect += 8;
				$testuser = 'postgres';
				## XXX Same as above - don't check unless user was problem
				eval {
					$dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS},
										{RaiseError => 1, PrintError => 0, AutoCommit => 1});
				};
				last GETHANDLE if ! $@;
			}
		}

		## Cannot connect to an existing database, so we'll create our own
		if ($arg->{nocreate}) {
			return $helpconnect, '', undef;
		}

		my ($info,$testport);
		$helpconnect = 16;

		## Do we have initdb available?
		$info = '';
		eval {
			$info = qx{initdb --help 2>&1};
		};
		last GETHANDLE if $@;

		if ($info !~ /\@postgresql\.org/) {
			$@ = 'Bad initdb output';
			last GETHANDLE;
		}

		## initdb seems to be available, let's use it to create a new cluster
		warn "Please wait, creating new database for testing\n";
		$info = '';
		eval {
			$info = qx{initdb -D $test_database_dir 2>&1};
		};
		last GETHANDLE if $@;

		if ($info =~ /FATAL/) {
			$@ = "initdb gave a FATAL error: $info";
			last GETHANDLE;
		}

		## initdb and pg_ctl cannot be run as root, so let's handle that
		if ($info =~ /run as root/) {
			eval {
				require File::Temp;
			};
			if ($@) {
				$@ = 'File::Temp required to safely create non-root-owned test directory';
				last GETHANDLE;
			}
			$test_database_dir =
				File::Temp::tempdir('dbdpg_testing_XXXXXXX', CLEANUP => 0, TMPDIR => 1);
			my $readme = "$test_database_dir/README";
			if (open my $fh, '>', $readme) {
				print $fh "This is a test directory for DBD::Pg and may be removed\n";
				print $fh "You may want to ensure the postmaster has been stopped first.\n";
				print $fh "Check the port in the postgresql.conf file\n";
				close $fh;
			}
			my $founduser = 0;
			$su = $testuser = '';
			for my $user (qw/postgres postgresql pgsql/) {
				my $uid = (getpwnam $user)[2];
				next if !defined $uid;
				next unless chown $uid, -1, $test_database_dir;
				$su = $user;
				$founduser++;
				$info = '';
				eval {
					$info = qx{su -l $user -c "initdb -D $test_database_dir" 2>&1};
				};
				if (!$@ and $info =~ /owned by user "$user"/) {
					$testuser = $user;
					last;
				}
			}
			if (!$founduser) {
				$@ = 'Unable to find a user to run initdb as';
				last GETHANDLE;
			}
			if (!$testuser) {
				$@ = "Failed to run initdb as user $su: $@";
				last GETHANDLE;
			}
			## At this point, both $su and $testuser are set
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
			$info = qx{netstat -lnx};
		};
		if ($@) {
			warn "netstat call failed, trying port $testport\n";
		}
		else {
			## Start at 5440 and go up until we are free
			$testport = 5440;
			my $maxport = 5470;
			{
				last if $info !~ /PGSQL\.$testport$/m;
				last if ++$testport >= $maxport;
				redo;
			}
			if ($testport >= $maxport) {
				$@ = "No free ports found for testing: tried 5442 to $maxport\n";
				last GETHANDLE;
			}
		}

		$@ = '';
		## Change to this new port and fire it up
		my $conf = "$test_database_dir/postgresql.conf";
		my $cfh;
		if (! open $cfh, '>>', $conf) {
			$@ = qq{Could not open "$conf": $!};
			last GETHANDLE;
		}
		print $cfh "\n\n## DBD::Pg testing port\nport=$testport\n\n";
		close $cfh;

		## Attempt to start up the test server
		$info = '';
		my $COM = qq{pg_ctl -l $test_database_dir/dbdpg_test.logfile -D $test_database_dir start};
		if ($su) {
			$COM = qq{su -l $su -c "$COM"};
		}
		eval {
			$info = qx{$COM};
		};
		if ($@ or $info !~ /\w/) {
			$@ = "Could not startup new database ($@) ($info)";
			last GETHANDLE;
		}

		## Attempt to connect to this server
		sleep 1;
		$testdsn = "dbi:Pg:dbname=postgres;port=$testport";
		my $loop = 1;
	  STARTUP: {
			eval {
				$dbh = DBI->connect($testdsn, $testuser, '',
									{RaiseError => 1, PrintError => 0, AutoCommit => 1});
			};
			if ($@ =~ /starting up/ or $@ =~ /PGSQL\.$testport/) {
				if ($loop++ < 20) {
					sleep 1;
					redo STARTUP;
				}
			}
			last GETHANDLE;
		}

	} ## end of GETHANDLE

	## At this point, we've got a connection, or have failed
	## Either way, we record for future runs

	if (open my $fh, '>', $helpfile) {
		print $fh "## This is a temporary file created for testing DBD::Pg\n";
		print $fh "## Created: " . scalar localtime() . "\n";
		print $fh "## Feel free to remove it!\n";
		print $fh "## Helpconnect: $helpconnect\n";
		if ($@) {
			print $fh "## DSN: FAIL!\n";
			print $fh "## ERROR: $@\n";
		}
		else {
			print $fh "## DSN: $testdsn\n";
			print $fh "## User: $testuser\n";
			print $fh "## Testdir: $test_database_dir\n" if 16 == $helpconnect;
			print $fh "## Testowner: $su\n" if $su;
		}
		close $fh;
	}

  GOTDBH:
	## This allows things like data_sources() to work if we did an initdb
	$ENV{DBI_DSN} = $testdsn;
	$ENV{DBI_USER} = $testuser;

	if ($arg->{nosetup}) {
		return $helpconnect, '', $dbh unless schema_exists($dbh, $S);
		$dbh->do("SET search_path TO $S");
	}
	else {

		cleanup_database($dbh);

		eval {
			$dbh->do("CREATE SCHEMA $S");
		};
		$@ and return $helpconnect, $@, undef;
		$dbh->do("SET search_path TO $S");
		$dbh->do('CREATE SEQUENCE dbd_pg_testsequence');
		# If you add columns to this, please do not use reserved words!
		my $SQL = q{
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
  "CaseTest" boolean,
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


sub get_test_settings {

	## Returns the DSN and user from the testfile if it exists
	## Defaults to ENV variables or blank

	my ($testdsn, $testuser, $testdir) = ('','','');
	my ($helpconnect, $su) = (0,'');
	if (-e $helpfile) {
		open my $fh, '<', $helpfile or die qq{Could not open "$helpfile": $!\n};
		while (<$fh>) {
			/DSN: (.+)/          and $testdsn = $1;
			/User: (\w+)/        and $testuser = $1;
			/Helpconnect: (\d+)/ and $helpconnect = $1;
			/Testowner: (\w+)/   and $su = $1;
			/Testdir: (.+)/      and $testdir = $1;
		}
		close $fh or die qq{Could not close "$helpfile": $!\n};
	}

	return $testdsn, $testuser, $helpconnect, $su, $testdir;
}


sub schema_exists {

	my ($dbh,$schema) = @_;
	my $SQL = 'SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?';
	my $sth = $dbh->prepare_cached($SQL);
	my $count = $sth->execute($schema);
	$sth->finish();
	return $count < 1 ? 0 : 1;

}


sub relation_exists {

	my ($dbh,$schema,$name) = @_;
	my $SQL = 'SELECT 1 FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n '.
		'WHERE n.oid=c.relnamespace AND n.nspname = ? AND c.relname = ?';
	my $sth = $dbh->prepare_cached($SQL);
	my $count = $sth->execute($schema,$name);
	$sth->finish();
	return $count < 1 ? 0 : 1;

}

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

}

sub shutdown_test_database {

	if (-e $test_database_dir) {
		eval {
			qx{pg_ctl -D $test_database_dir -m fast stop};
		};
		return $@;
	}

	my ($testdsn,$testuser,$helpconnect,$su,$testdir) = get_test_settings();
	if ($testdir) {
		my $COM = "pg_ctl -D $testdir -m fast stop";
		if ($su) {
			$COM = qq{su -l $su -c "$COM"};
		}
		warn "Shutting down test database\n";
		eval {
			qx{$COM};
		};
		return $@;
	}

}

1;
