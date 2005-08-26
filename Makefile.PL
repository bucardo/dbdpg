
# $Id$

use ExtUtils::MakeMaker;
use Config;
use strict;
use 5.006001;
use DBI 1.38;
use DBI::DBD;

my $VERSION = "1.43_1";

my $lib;
BEGIN {
    my %sep = (MacOS   => ':',
               MSWin32 => '\\',
               os2     => '\\',
               VMS     => '\\',
               NetWare => '\\',
               dos     => '\\');
    my $s = $sep{$^O} || '/';
    $lib = join $s, 't', 'lib';
}

use lib $lib;
print "Configuring DBD::Pg $VERSION\n";
print "Remember to actually read the README file!\n";

my $POSTGRES_INCLUDE;
my $POSTGRES_LIB;

# We need the version information to properly set compiler options later
# Use App::Info to get the data we need.
require App::Info::RDBMS::PostgreSQL;
require App::Info::Handler::Prompt;
my $p = App::Info::Handler::Prompt->new;
my $pg = App::Info::RDBMS::PostgreSQL->new(on_unknown => $p);
my ($major_ver, $minor_ver, $patch) = map {$pg->$_} qw/major_version minor_version patch_version/;
my $serverversion = sprintf("%d%.02d%.02d", $major_ver, $minor_ver, $patch);

if ((!$ENV{POSTGRES_INCLUDE} or !$ENV{POSTGRES_LIB}) and !$ENV{POSTGRES_HOME}) {
    $POSTGRES_INCLUDE = $pg->inc_dir;
    $POSTGRES_LIB     = $pg->lib_dir;
} elsif ((!$ENV{POSTGRES_INCLUDE} or !$ENV{POSTGRES_LIB}) and $ENV{POSTGRES_HOME}) {
    $POSTGRES_INCLUDE = "$ENV{POSTGRES_HOME}/include";
    $POSTGRES_LIB     = "$ENV{POSTGRES_HOME}/lib";
} else {
    $POSTGRES_INCLUDE = "$ENV{POSTGRES_INCLUDE}";
    $POSTGRES_LIB     = "$ENV{POSTGRES_LIB}";
}

my $os = $^O;
print "OS: $os\n";
print "PostgreSQL version: $serverversion\n";

if ($os =~ /Win32/) {
	for ($POSTGRES_INCLUDE, $POSTGRES_LIB) {
		$_ = qq{"$_"} if index($_,'"');
	}
}

## Warn about older versions
if ($serverversion < 70200) {
	print "\n****************\n";
	print "WARNING! DBD::Pg no longer supports versions less than 7.2.\n";
	print "It is highly recommended that you upgrade PostgreSQL to a newer version.\n";
	print "****************\n\n";
}

if ($serverversion < 70400) {
	print "\n****************\n";
	print "WARNING! Support for servers older than version 7.4 will soon end.\n";
	print "It is highly recommended that you upgrade PostgreSQL to a newer version.\n";
	print "****************\n\n";
}

my $dbi_arch_dir;
if ($os eq 'MSWin32') {
	$dbi_arch_dir = "\$(INSTALLSITEARCH)/auto/DBI";
}
else {
	{
		local *STDOUT; ## Prevent duplicate debug info as WriteMakefile also calls this
		$dbi_arch_dir = dbd_dbi_arch_dir();
  }
}

my $comp_opts = $Config{q{ccflags}} . " -DPGLIBVERSION=$serverversion";

my %opts = 
	(
	 NAME           => 'DBD::Pg',
	 VERSION_FROM   => 'Pg.pm',
	 INC            => "-I$POSTGRES_INCLUDE -I$dbi_arch_dir",
	 OBJECT         => "Pg\$(OBJ_EXT) dbdimp\$(OBJ_EXT) quote\$(OBJ_EXT) types\$(OBJ_EXT)",
	 LIBS           => ["-L$POSTGRES_LIB -lpq"],
	 AUTHOR         => 'http://gborg.postgresql.org/project/dbdpg/',
	 ABSTRACT       => 'PostgreSQL database driver for the DBI module',
	 PREREQ_PM      => { 'Test::Simple' => '0.17',  # Need Test::More.
											 'Test::Harness' => '2.03', # Need TODO tests.
									 },
	 CCFLAGS        => $comp_opts,
	 PERL_MALLOC_OK => 1,
	 NEEDS_LINKING  => 1,
	 clean          => { FILES => 'trace' },
);

if ($os eq 'hpux') {
    my $osvers = $Config{osvers};
    if ($osvers < 10) {
        print "Warning: Forced to build static not dynamic on $os $osvers.\a\n";
        $opts{LINKTYPE} = 'static';
    }
}
elsif ($os =~ /Win32/) {
	my $msdir = $POSTGRES_LIB;
	$msdir =~ s#"$#/ms"#;
	$opts{LIBS}[0] .= " -L$msdir";
}

if ($Config{dlsrc} =~ /dl_none/) {
    $opts{LINKTYPE} = 'static';
}

sub MY::postamble {
	my $string = &dbd_postamble;
	## Evil, evil stuff - but we really want to suppress the "duplicate function" message!
	$string =~ s/dependancy/dependency/g; ## why not, while we are here
	$string =~ s#(BASEEXT\)/g)#$1; s/^do\\\(/dontdo\\\(/#;
	return $string;
}

WriteMakefile(%opts);

exit(0);

# end of Makefile.PL
