
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

my $S = 'dbd_pg_testschema';

sub connect_database {

	## Connect to the database (unless 'dbh' is passed in)
	## Setup all the tables (unless 'nosetup' is passed in)
	## Returns three values:
	## 1. helpconnect for use by 01connect.t
	## 2. Any error generated
	## 3. The database handle, or undef
	## The returned handle has AutoCommit=0 (unless AutoCommit is passed in)

	my $arg = shift || {};
	ref $arg and ref $arg eq 'HASH' or die qq{Need a hashref!\n};

	my $dbh = $arg->{dbh} || '';

	my $helpconnect = 0;
	if (!defined $ENV{DBI_DSN}) {
		$helpconnect = 1;
		$ENV{DBI_DSN} = 'dbi:Pg:';
	}

	if (!$dbh) {
		eval {
			$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
								{RaiseError => 1, PrintError => 0, AutoCommit => 1});
		};
		if ($@) {
			return $helpconnect, $@, undef if $@ !~ /FATAL/ or defined $ENV{DBI_USER};
			## Try one more time as postgres user (and possibly database)
			if ($helpconnect) {
				$ENV{DBI_DSN} .= 'dbname=postgres';
				$helpconnect += 2;
			}
			$helpconnect += 4;
			$ENV{DBI_USER} = $^O =~ /bsd/ ? '_postgresql' : 'postgres';
			eval {
				$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
									{RaiseError => 1, PrintError => 0, AutoCommit => 1});
			};
			if ($@) {
				## Try one final time for Beastie
				if ($ENV{DBI_USER} ne 'postgres') {
					$helpconnect += 8;
					$ENV{DBI_USER} = 'postgres';
					eval {
						$dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
											{RaiseError => 1, PrintError => 0, AutoCommit => 1});
					};
				}
				return $helpconnect, $@, undef if $@;
			}
		}
	}
	if ($arg->{nosetup}) {
		return $helpconnect, undef, $dbh unless schema_exists($dbh, $S);
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
	return $helpconnect, undef, undef;
}

$dbh->{AutoCommit} = 0 unless $arg->{AutoCommit};
return $helpconnect, undef, $dbh;

} ## end of connect_database


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

	my $dbh = shift;
	my $type = shift || 0;

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

1;
