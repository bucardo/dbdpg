# $Id$

use ExtUtils::MakeMaker;
use Config;
use strict;
use warnings;
use 5.006001;

## No version.pm for this one, as the prereqs are not loaded yet.
my $VERSION = '2.5.2';

my $lib;
BEGIN {
    my %sep = (
			   MacOS   => ':',
               MSWin32 => '\\',
               os2     => '\\',
               VMS     => '\\',
               NetWare => '\\',
               dos     => '\\',
			   );
    my $s = $sep{$^O} || '/';
    $lib = join $s, 't', 'lib';
}

use lib $lib;
if ($VERSION =~ /_/) {
	print "WARNING! This is a test version ($VERSION) and should not be used in production!\n";
}

if (grep { /help/ } @ARGV) {
	print qq{
Usage: perl $0

No other options are necessary, although you may need to
set some evironment variables. See the README file for full details.

In brief:

By default Makefile.PL uses App::Info to find the location of the
PostgreSQL library and include directories.  However, if you want to
control it yourself, define the environment variables POSTGRES_INCLUDE
and POSTGRES_LIB, or define just POSTGRES_HOME. Note that if you have
compiled PostgreSQL with SSL support, you must define the POSTGRES_LIB
environment variable and add "-lssl" to it, like this:

export POSTGRES_LIB="/usr/local/pgsql/lib -lssl"

The usual steps to install DBD::Pg:

1.   perl Makefile.PL
2.   make
3.   make test
4.   make install

Do steps 1 to 3 as a normal user, not as root!

If all else fails, email dbd-pg\@perl.org for help.

};
	exit 1;

}

print "Configuring DBD::Pg $VERSION\n";

my $POSTGRES_INCLUDE;
my $POSTGRES_LIB;

# We need the version information to properly set compiler options later
# Use App::Info to get the data we need.
require App::Info::RDBMS::PostgreSQL;
require App::Info::Handler::Prompt;
my $p = App::Info::Handler::Prompt->new;
my $pg = App::Info::RDBMS::PostgreSQL->new(on_unknown => $p);
my ($major_ver, $minor_ver, $patch, $conf) = map {$pg->$_} qw/major_version minor_version patch_version configure/;
my $serverversion = sprintf '%d%.02d%.02d', $major_ver, $minor_ver, $patch;
my $defaultport = $conf =~ /with-pgport=(\d+)/ ? $1 : 5432;

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
print "PostgreSQL version: $serverversion (default port: $defaultport)\n";
my $showhome = $ENV{POSTGRES_HOME} || '(not set)';
print "POSTGRES_HOME: $showhome\n";
my $showinc = $ENV{POSTGRES_INCLUDE} || '(not set)';
print "POSTGRES_INCLUDE: $showinc\n";
my $showlib = $ENV{POSTGRES_LIB} || '(not set)';
print "POSTGRES_LIB: $showlib\n";
print "OS: $os\n";

my $baddir;
for my $d (qw/HOME INCLUDE/) {
	next if ! exists $ENV{"POSTGRES_$d"};
	if (! -d $ENV{"POSTGRES_$d"}) {
		printf "The environment variable %s points to a non-existent directory: %s\n",
			"POSTGRES_$d", $ENV{"POSTGRES_$d"};
		$baddir++;
	}
}
if ($baddir) {
	print "Cannot build unless the directories exist, exiting.\n";
	exit 0;
}

if ($serverversion < 11) {
	print "Could not determine the PostgreSQL library version.\n".
		"Please ensure that a valid path is given to the 'pg_config' command,\n".
			"either manually or by setting the environment variables\n".
				"POSTGRES_DATA, POSTGRES_INCLUDE, and POSTGRES_LIB\n";
	exit 0;
}

if ($os =~ /Win32/) {
	for ($POSTGRES_INCLUDE, $POSTGRES_LIB) {
		$_ = qq{"$_"} if index $_,'"';
	}
}

## Warn about older versions
if ($serverversion < 70400) {
	print "\n****************\n";
	print "WARNING! DBD::Pg no longer supports versions less than 7.4.\n";
	print "You must upgrade PostgreSQL to a newer version.\n";
	print "****************\n\n";
	exit 1;
}

my $dbi_arch_dir;
{
	eval {
		require DBI::DBD;
	};
	if ($@) {
		print "Could not load DBI::DBD - is the DBI module installed?\n";
		exit 0;
	}
	local *STDOUT; ## Prevent duplicate debug info as WriteMakefile also calls this
	$dbi_arch_dir = DBI::DBD::dbd_dbi_arch_dir();
}

my $defines = " -DPGLIBVERSION=$serverversion -DPGDEFPORT=$defaultport";
my $comp_opts = $Config{q{ccflags}} . $defines;

if ($ENV{DBDPG_GCCDEBUG}) {
	warn "Enabling many compiler options\n";
	$comp_opts .= ' -Wchar-subscripts -Wcomment';
	$comp_opts .= ' -Wformat=2'; ## does -Wformat,-Wformat-y2k,-Wformat-nonliteral,-Wformat-security
	$comp_opts .= ' -Wnonnull';
	$comp_opts .= ' -Wuninitialized -Winit-self'; ## latter requires the former
	$comp_opts .= ' -Wimplicit'; ## does -Wimplicit-int and -Wimplicit-function-declaration
	$comp_opts .= ' -Wmain -Wmissing-braces -Wparentheses -Wsequence-point -Wreturn-type -Wswitch -Wswitch-enum -Wtrigraphs';
	$comp_opts .= ' -Wunused'; ## contains -Wunused- function,label,parameter,variable,value
	$comp_opts .= ' -Wunknown-pragmas -Wstrict-aliasing';
	$comp_opts .= ' -Wall'; ## all of above, but we enumerate anyway
	$comp_opts .= ' -Wextra -Wdeclaration-after-statement -Wendif-labels -Wpointer-arith';
	$comp_opts .= ' -Wbad-function-cast -Wcast-qual -Wcast-align -Wsign-compare -Waggregate-return';
	$comp_opts .= ' -Wmissing-prototypes -Wmissing-declarations -Wmissing-format-attribute -Wpacked -Winline -Winvalid-pch';
	$comp_opts .= ' -Wdisabled-optimization'; ## Not terribly useful
	$comp_opts .= ' -Wnested-externs'; ## Does not like Perl__notused (from DBIXS;)
}

my %opts =
	(
	 NAME           => 'DBD::Pg',
	 VERSION_FROM   => 'Pg.pm',
	 INC            => "-I$POSTGRES_INCLUDE -I$dbi_arch_dir",
	 OBJECT         => "Pg\$(OBJ_EXT) dbdimp\$(OBJ_EXT) quote\$(OBJ_EXT) types\$(OBJ_EXT)",
	 LIBS           => ["-L$POSTGRES_LIB -lpq"],
	 AUTHOR         => 'Greg Sabino Mullane',
	 ABSTRACT       => 'PostgreSQL database driver for the DBI module',
	 PREREQ_PM      => {
						'ExtUtils::MakeMaker' => '6.11',
						'DBI'                 => '1.52',
						'Test::More'          => '0.61',
						'version'             => '0',
					   },
	 CCFLAGS        => $comp_opts,
	 PERL_MALLOC_OK => 1,
	 NEEDS_LINKING  => 1,
	 NO_META        => 1,
	 clean          => { FILES => 'trace Pg.xsi' },
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
	$msdir =~ s{"$}{/ms"};
	$opts{LIBS}[0] .= " -L$msdir";
}

if ($Config{dlsrc} =~ /dl_none/) {
    $opts{LINKTYPE} = 'static';
}

sub MY::postamble { ## no critic ProhibitQualifiedSubDeclarations
	no strict 'subs'; ## no critic ProhibitNoStrict
	my $string = DBI::DBD->dbd_postamble();
	use strict 'subs';
	## Evil, evil stuff - but we really want to suppress the "duplicate function" message!
	$string =~ s/dependancy/dependency/g; ## why not, while we are here
	$string =~ s{(BASEEXT\)/g)}{$1; s/^do\\\(/dontdo\\\(/};

        my $tags = <<'MAKE_FRAG';
.PHONY: tags

tags:
	ctags -f tags --recurse --totals \
		--exclude=blib \
		--exclude=.svn \
		--exclude='*~' \
		--languages=Perl,C --langmap=c:+.h,Perl:+.t \

MAKE_FRAG
        $string = "$string\n$tags\n";

	$string .= <<'MAKE_SPLINT';

## This must be version 3.1.2 or better: earlier versions have many 
## problems parsing the DBI header files
SPLINT = splint

## Temp directory, for use with +keep
SPLINT_TMP = $(TMP)/splint_dbdpg

SPLINTFLAGS =            \
  -message-stream-stdout \
  -linelen 90            \
  -boolops               \
  -tmpdir $(SPLINT_TMP)  \
  +posixstrictlib        \
  +ignoresigns           \
  +showdeephistory       \
  -predboolint           \
  -nullpass              \
  +charint               \

SPLINTFLAGS_TEST = 

SDEFINES = 

CCFLAGS2 = -D_REENTRANT -D_GNU_SOURCE -DTHREADS_HAVE_PIDS -DDEBIAN -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DPGLIBVERSION=80111 -DPGDEFPORT=5432

splint: $(H_FILES) $(C_FILES)
	$(MKPATH) $(SPLINT_TMP)
	$(SPLINT) $(SPLINTFLAGS) $(SPLINTFLAGS_TEST) $(SDEFINES) -I$(PERL_INC) $(INC) $(C_FILES)

MAKE_SPLINT

	$string =~ s/SDEFINES = /SDEFINES =$defines/;

	return $string;
}

my $output = WriteMakefile(%opts);

if (!exists $output->{EXTRALIBS} or
	($output->{EXTRALIBS} !~ /\-lpq/ and $output->{EXTRALIBS} !~ /libpq/)) {

	my $makefile = exists $output->{MAKEFILE}
		? "\nRemoving ($output->{MAKEFILE})\n" : '';

	warn qq{
==========================================================

WARNING! No libpq libraries were detected!

You need to install the postgresql-libs package for your system,

or set the POSTGRES_LIB environment variable to the correct place.
$makefile
===========================================================

};

	## Do not let make proceed
	unlink $output->{MAKEFILE} if $makefile;

	exit 1;
}

exit 0;

# end of Makefile.PL
