
# $Id$

use ExtUtils::MakeMaker;
use Config;
use strict;
use 5.006001;
use DBI 1.35;
use DBI::DBD;

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
print "Configuring Pg\n";
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
print "PostgreSQL version: $major_ver.$minor_ver.$patch\n";

if ($os =~ /Win32/) {
	for ($POSTGRES_INCLUDE, $POSTGRES_LIB) {
		$_ = qq{"$_"} if index($_,'"');
	}
}

## Warn about older versions
if ($major_ver < 7 or ($major_ver == 7 and $minor_ver < 2)) {
	print "\n****************\n";
	print "WARNING! DBD::Pg no longer supports versions less than 7.2.\n";
	print "It is highly recommended that you upgrade PostgreSQL to a newer version.\n";
	print "****************\n\n";
}

my $dbi_arch_dir;
if ($os eq 'MSWin32') {
	$dbi_arch_dir = "\$(INSTALLSITEARCH)/auto/DBI";
}
else {
  $dbi_arch_dir = dbd_dbi_arch_dir();
}

my $comp_opts = $Config{q{ccflags}};
if ($major_ver == 7) {
    if ($minor_ver >= 2) {
        $comp_opts .= " -DHAVE_PQescapeString";
        $comp_opts .= " -DHAVE_PQescapeBytea";
    }
    if ($minor_ver >= 3) {
        $comp_opts .= " -DHAVE_PQunescapeBytea";
    }
    if ($minor_ver >= 4) {
        $comp_opts .= " -DHAVE_PQfreemem";
    }
} elsif ($major_ver > 7) {
        $comp_opts .= " -DHAVE_PQescapeString -DHAVE_PQescapeBytea";
        $comp_opts .= " -DHAVE_PQunescapeBytea -DHAVE_PQfreemem";
}

if ($major_ver > 7 or ($major_ver == 7 and $minor_ver >= 4)) {
	$comp_opts .= " -DHAVE_PQprotocol";
}
if ($major_ver >= 8) {
	$comp_opts .= " -DPG_VERSION8";
}

my %opts = (
    NAME         => 'DBD::Pg',
    VERSION_FROM => 'Pg.pm',
    INC          => "-I$POSTGRES_INCLUDE -I$dbi_arch_dir",
    OBJECT       => "Pg\$(OBJ_EXT) dbdimp\$(OBJ_EXT) quote\$(OBJ_EXT) types\$(OBJ_EXT)",
    LIBS         => ["-L$POSTGRES_LIB -lpq"],
    AUTHOR       => 'http://gborg.postgresql.org/project/dbdpg/',
    ABSTRACT     => 'PostgreSQL database driver for the DBI module',
    PREREQ_PM    => { 'Test::Simple' => '0.17',  # Need Test::More.
                      'Test::Harness' => '2.03', # Need TODO tests.
                    },
    CCFLAGS      => $comp_opts,
    PERL_MALLOC_OK => 1,
);

if ($os eq 'hpux') {
    my $osvers = $Config{osvers};
    if ($osvers < 10) {
        print "Warning: Forced to build static not dynamic on $os $osvers.\a\n";
        $opts{LINKTYPE} = 'static';
    }
}

if ($Config{dlsrc} =~ /dl_none/) {
    $opts{LINKTYPE} = 'static';
}

sub MY::postamble { return &dbd_postamble; }


WriteMakefile(%opts);

exit(0);

# end of Makefile.PL
