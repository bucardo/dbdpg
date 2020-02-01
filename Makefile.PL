use ExtUtils::MakeMaker;
use Config;
use strict;
use warnings;
use 5.008001;

## No version.pm for this one, as the prereqs are not loaded yet.
my $VERSION = '3.10.4';

## App::Info is stored inside t/lib
## Create a proper path so we can use it below
my $lib;
BEGIN {
    use vars qw/$sep/;
    my %sep = (
               MacOS   => ':',
               MSWin32 => '\\',
               os2     => '\\',
               VMS     => '\\',
               NetWare => '\\',
               dos     => '\\',
               );
    $sep = $sep{$^O} || '/';
    $lib = join $sep, 't', 'lib';
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
my $prompt;
if ($ENV{PERL_MM_USE_DEFAULT} or $ENV{AUTOMATED_TESTING}) {
    require App::Info::Handler::Print;
    $prompt = App::Info::Handler::Print->new;
}
else {
    require App::Info::Handler::Prompt;
    $prompt = App::Info::Handler::Prompt->new;
}

my $pg = App::Info::RDBMS::PostgreSQL->new(on_unknown => $prompt);
my ($major_ver, $minor_ver, $patch, $conf, $bindir) = map {$pg->$_}
    qw/major_version minor_version patch_version configure bin_dir/;
my $initdb = '';
if (defined $bindir and -d $bindir) {
    my $testinitdb = "$bindir${sep}initdb";
    if (-e $testinitdb) {
        $initdb = $testinitdb;
    }
}
my $serverversion = 0;
my $defaultport = 0;

if (defined $major_ver) {
    $serverversion = sprintf '%d%.02d%.02d', $major_ver, $minor_ver, $patch;
    $defaultport = $conf =~ /with-pgport=(\d+)/ ? $1 : 5432;
}

# We set POSTGRES_INCLUDE and POSTGRES_LIB from the first found of:
# 1. environment variable
# 2. App::Info::RDBMS::PostgreSQL information
# 3. subdirectory of $ENV{POSTGRES_HOME}

$POSTGRES_INCLUDE = $ENV{POSTGRES_INCLUDE} || $pg->inc_dir;

if (! defined $POSTGRES_INCLUDE) {
	if (! defined $ENV{POSTGRES_HOME}) {
		warn "No POSTGRES_HOME defined, cannot find automatically\n";
		exit 0;
	}
	$POSTGRES_INCLUDE = "$ENV{POSTGRES_HOME}/include";
}

$POSTGRES_LIB = $ENV{POSTGRES_LIB} || $pg->lib_dir || "$ENV{POSTGRES_HOME}/lib";

my $os = $^O;
print "PostgreSQL version: $serverversion (default port: $defaultport)\n";
my $showhome = $ENV{POSTGRES_HOME} || '(not set)';
print "POSTGRES_HOME: $showhome\n";
my $showinc = $POSTGRES_INCLUDE || '(not set)';
print "POSTGRES_INCLUDE: $showinc\n";
my $showlib = $POSTGRES_LIB || '(not set)';
print "POSTGRES_LIB: $showlib\n";
print "OS: $os\n";

my $baddir = 0;
sub does_path_exist {
    my ($path_name, $path) = @_;

    return if ! defined $path or ! length $path or -d $path;
    printf "The value of %s points to a non-existent directory: %s\n",
        $path_name, $path;
    $baddir++;
    return;
}

does_path_exist('POSTGRES_HOME', $ENV{POSTGRES_HOME});
does_path_exist('POSTGRES_INCLUDE',  $POSTGRES_INCLUDE);

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
if ($serverversion < 80000) {
    print "\n****************\n";
    print "WARNING! DBD::Pg no longer supports versions less than 8.0.\n";
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
    $comp_opts .= ' -Wextra -Wendif-labels -Wpointer-arith';
    $comp_opts .= ' -Wbad-function-cast -Wcast-qual -Wcast-align -Wsign-compare -Waggregate-return';
    $comp_opts .= ' -Wmissing-prototypes -Wmissing-declarations -Wmissing-format-attribute -Wpacked -Winline -Winvalid-pch';
    $comp_opts .= ' -Wdisabled-optimization';
    $comp_opts .= ' -Wnested-externs';
    $comp_opts .= " -Wstrict-prototypes"; ## Still hits a couple places in types.h
    $comp_opts .= " -Wswitch-default";
    $comp_opts .= " -Wsystem-headers";
    $comp_opts .= " -Wmissing-noreturn";
    $comp_opts .= " -Wfloat-equal"; ## Does not like SvTRUE() calls
}

my %opts =
    (
     NAME           => 'DBD::Pg',
     VERSION_FROM   => 'Pg.pm',
     INC            => "-I$POSTGRES_INCLUDE -I$dbi_arch_dir",
     OBJECT         => "Pg\$(OBJ_EXT) dbdimp\$(OBJ_EXT) quote\$(OBJ_EXT) types\$(OBJ_EXT)",
     LIBS           => ["-L$POSTGRES_LIB -lpq -lm"],
     AUTHOR         => 'Greg Sabino Mullane',
     ABSTRACT       => 'PostgreSQL database driver for the DBI module',
     PREREQ_PM      => {
                        'ExtUtils::MakeMaker' => '6.11',
                        'DBI'                 => '1.614',
                        'Test::More'          => '0.88',
                        'Time::HiRes'         => '0',
                        'version'             => '0',
                       },
     CCFLAGS        => $comp_opts,
     PERL_MALLOC_OK => 1,
     NEEDS_LINKING  => 1,
     NO_META        => 1,
     NORECURS       => 1,
     PM             => {
                        'Pg.pm' => '$(INST_LIBDIR)/Pg.pm',
                        'lib/Bundle/DBD/Pg.pm' => '$(INST_LIB)/Bundle/DBD/Pg.pm',
                       },
     clean          => { FILES => 'trace Pg.xsi README.testdatabase cover_db' },
     realclean      => { FILES => 'dbdpg_test_database/' },
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
    $opts{LIBS}[0] .= " -L$msdir -lsecur32";
}

if ($Config{dlsrc} =~ /dl_none/) {
    $opts{LINKTYPE} = 'static';
}

{
    package MY; ## no critic
    sub MY::test { ## no critic
        my $string = shift->SUPER::test(@_);
        $string =~ s/(PERL_DL_NONLAZY=1)/PGINITDB="$initdb" $1/g;
        return "HARNESS_OPTIONS=j1\n$string";
    }
}

sub constants {
    my $self = shift;

    my $old_constants = $self->SUPER::constants();
    my $new_constants = '';
    for my $line (split /\n/ => $old_constants) {
        if ($line =~ /^INC = .*strawberry.*/ ) {
            print qq(Strawberry Perl found; adjusting the INC variable;\n);
            $line .= ' -I ' . DBI::DBD::dbd_dbi_arch_dir();
            print qq(INC is now $line\n);
        }
        $new_constants .= "$line\n";
    }
    return $new_constants;
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
		--exclude=.git \
		--exclude='*~' \
		--languages=Perl,C --langmap=c:+.h,Perl:+.t \

MAKE_FRAG
        $string = "$string\n$tags\n";

	$string .= <<'MAKE_SPLINT';

## This must be version 3.2.1 or better: earlier versions have many 
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
  +boolint               \
  +allglobals            \

SPLINTFLAGS_TEST = 

SDEFINES = 

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
