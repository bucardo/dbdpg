#!perl

## Various code cleanup checks

use 5.008001;
use strict;
use warnings;
use Test::More;
use File::Find;

my (@testfiles,@perlfiles,@cfiles,@headerfiles,%fileslurp,$t);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
}

$ENV{LANG} = 'C';
find (sub { push @cfiles      => $File::Find::name if /^[^.].+\.c$/ and $_ ne 'Pg.c' and $File::Find::dir !~ /tmp|DBD-Pg/; }, '.');
find (sub { push @headerfiles => $File::Find::name if /^[^.].+\.h$/ and $_ ne 'dbivport.h' and $File::Find::dir !~ /tmp/; }, '.');
find (sub { push @testfiles   => $File::Find::name if /^[^.]\w+\.(t|pl)$/; }, 't');
find (sub { push @perlfiles   => $File::Find::name if /^[^.].+\.(pm|pl|t)$/ and $File::Find::dir !~ /tmp/; }, '.');

##
## Load all Test::More calls into memory
##
my $testmore = 0;
for my $file (@testfiles) {
    open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
    my $line;
    while (defined($line = <$fh>)) {
        last if $line =~ /__DATA__/; ## perlcritic.t
        for my $func (qw/ok isnt pass fail cmp cmp_ok is_deeply unlike like/) { ## no skip
            next if $line !~ /\b$func\b/;
            next if $line =~ /$func \w/; ## e.g. 'skip these tests'
            next if $line =~ /[\$\%]$func/; ## e.g. $ok %ok
            next if $line =~ /['"][^'"]*$func/; ## e.g. 'like' in quotes
            $fileslurp{$file}{$.}{$func} = $line;
            $testmore++;
        }
    }
    close $fh or die qq{Could not close "$file": $!\n};
}

ok (@testfiles, 'Found files in test directory');

##
## Make sure the README.dev mentions all files used, and jives with the MANIFEST
##
my $file = 'README.dev';
open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
my $point = 1;
my %devfile;
while (<$fh>) {
    chomp;
    if (1 == $point) {
        next unless /File List/;
        $point = 2;
        next;
    }
    last if /= Compiling/;
    if (m{^([\w\./-]+) \- }) {
        $devfile{$1} = $.;
        next;
    }
    if (m{^(t/.+)}) {
        $devfile{$1} = $.;
    }
}
close $fh or die qq{Could not close "$file": $!\n};

$file = 'MANIFEST';
my %manfile;
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
    next unless /^(\S.+)/;
    $manfile{$1} = $.;
}
close $fh or die qq{Could not close "$file": $!\n};

$file = 'MANIFEST.SKIP';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
while (<$fh>) {
    next unless m{^(t/.*)};
    $manfile{$1} = $.;
}
close $fh or die qq{Could not close "$file": $!\n};


## Make sure each PQxx() call has a matching TRACE above it
for my $file (@cfiles) {
    open $fh, '<', $file or die qq{Could not open "$file": $!\n};
    my $lastline = '';
    my $lastline2 = '';
    my $traceok = 1;
    while (<$fh>) {
        chomp;
        if (/PQerrorMessage/) {
            if ($lastline !~ /TRACE_PQERRORMESSAGE/) {
                diag "Failure at line $. for PQerrorMessage\n";
            }
        }
        while (/\b(PQ[a-z]\w+)\(/g) {
            my $func = $1;
            ## Items small and rare enough not to trace:
            next if $func eq 'PQgetlength';
            next if $func eq 'PQgetvalue';
            next if $func eq 'PQbinaryTuples';
            next if $func eq 'PQflush';
            next if $func eq 'PQclosePrepared';

            my $uc = uc($func);
            next if $lastline =~ /TRACE_$uc\b/;
            next if $lastline !~ /;/ and $lastline2 =~ /TRACE_$uc\b/;

            diag "Failure at line $.: Found no matching TRACE for $func\n";
            $traceok = 0;
        }
        $lastline2 = $lastline;
        $lastline = $_;
    }
    close $fh;
    if ($traceok) {
        pass (qq{File "$file" has matching TRACE calls for each PQ function});
    }
    else {
        fail (qq{File "$file" does not have TRACE calls for each PQ function});
    }
}

##
## Everything in MANIFEST[.SKIP] should also be in README.dev
##
for my $file (sort keys %manfile) {
    if (!exists $devfile{$file}) {
        fail qq{File "$file" is in MANIFEST but not in README.dev\n};
    }
}

##
## Everything in README.dev should also be in MANIFEST, except special files
##
my %derived = map { $_, 1 } qw/Makefile Pg.c README.testdatabase dbdpg_test_database dbdpg_test_postgres_versions.pl/;
for my $file (sort keys %devfile) {
    if (!exists $manfile{$file} and !exists $derived{$file}) {
        fail qq{File "$file" is in README.dev but not in MANIFEST\n};
    }
    if (exists $manfile{$file} and exists $derived{$file}) {
        fail qq{File "$file" is derived and should not be in MANIFEST\n};
    }
}

## All files in the repo should be mentioned in the README.dev
my %gitfiles = map { chomp; $_, 1 } qx{git -c 'safe.directory=*' ls-files};
for my $file (sort keys %gitfiles) {
    next if $file =~ /^z_announcements/;
    next if $file =~ /^\.git/;
    if (!exists $devfile{$file}) {
        fail qq{File "$file" is in the repo but not in the README.dev file};
    }
}

##
## Make sure all Test::More function calls are standardized
##
for my $file (sort keys %fileslurp) {
    for my $linenum (sort {$a <=> $b} keys %{$fileslurp{$file}}) {
        for my $func (sort keys %{$fileslurp{$file}{$linenum}}) {
            $t=qq{Test::More method "$func" is in standard format inside $file at line $linenum};
            my $line = $fileslurp{$file}{$linenum}{$func};
            ## Must be at start of line (optional whitespace and comment), a space, a paren, and something interesting
            next if $line =~ /testmorewords/;
            next if $line =~ /\w+ fail/;
            next if $line =~ /defined \$expected \? like/;
            like ($line, qr{^\s*#?$func \(['\S]}, $t);
        }
    }
}

##
## Check C and Perl files for errant tabs
##
for my $file (@cfiles, @headerfiles, @perlfiles) {
    my $tabfail = 0;
    open my $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        $tabfail++ if /\t/;
    }
    close $fh;
    if ($tabfail) {
        fail (qq{File "$file" contains one or more tabs: $tabfail});
    }
    else {
        pass (qq{File "$file" has no tabs});
    }
}

##
## Make sure all Perl files request the same minimum version of Perl
##
my $firstversion = 0;
my %ver;
for my $file (@perlfiles) {

    ## The App::Info items do not need this check
    next if $file =~ m{/App/Info};

    ## Skip this one for now, it needs slightly higher version
    next if $file =~ /00_release/;

    open my $fh, '<', $file or die "Could not open $file: $!\n";
    my $minversion = 0;
    while (<$fh>) {
        if (/^use ([0-9]+\.[0-9]+);$/) {
            $minversion = $1;
            $firstversion ||= $minversion;
            $ver{$file} = $minversion;
            last;
        }
    }

    close $fh;
    if ($minversion) {
        pass (qq{Found a minimum Perl version of $minversion for the file $file});
    }
    else {
        fail (qq{Failed to find a minimum Perl version for the file $file});
    }
}

for my $file (sort keys %ver) {
    my $version = $ver{$file};
    if ($version eq $firstversion) {
        pass(qq{Correct minimum Perl version ($firstversion) for file $file});
    }
    else {
        fail(qq{Wrong minimum Perl version ($version is not $firstversion) for file $file});
    }
}

##
## Check for stale or duplicated spelling words
##
$file = 't/99_spellcheck.t';
open $fh, '<', $file or die "Could not open $file: $!\n";
1 while <$fh> !~ /__DATA__/;
my %word;
my $dupes = 0;
while (<$fh>) {
    next if /^#/ or /^\s*$/;
    chomp;
    $dupes++ if $word{$_}++;
}

$t = q{Number of duplicate spelling word entries is zero};
is ($dupes, 0, $t);

for my $file (qw{
    README Changes TODO README.dev README.win32 CONTRIBUTING.md
    Pg.pm Pg.xs dbdimp.c quote.c Makefile.PL Pg.h types.c dbdimp.h
    t/01connect.t t/01constants.t t/02attribs.t t/03dbmethod.t t/03smethod.t
    t/04misc.t t/12placeholders.t t/99_yaml.t
    testme.tmp.pl dbdpg_test_postgres_versions.pl
}) {
    open $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        s{([A-Za-z][A-Za-z']+)}{(my $x = $1) =~ s/'$//; delete $word{$x}; ' '}ge;
    }
}

$t = q{Number of unused spelling words is zero};
my $unused_words = keys %word;
is ($unused_words, 0, $t);

my $stop = 0;
for my $x (sort keys %word) {
    diag "Unused: $x\n";
    last if $stop++ > 10;
}

##
## Make sure all ENV calls in Perl files are known words
##
my $good_var_names = '
DBI_DSN DBI_USER DBI_PASS
DBDPG_INITDB DBDPG_DEBUG DBDPG_NOCLEANUP DBDPG_TESTINITDB DBDPG_TEST_ALWAYS_ENV DBDPG_TEST_NOHELPFILE DBDPG_TEMPDIR
POSTGRES_HOME PGDATABASE PGINITDB
LANG USER
AUTHOR_TESTING RELEASE_TESTING TEST_CRITIC_SKIPNONTEST TEST_OUTPUT TEST_SIGNATURE
';
my %valid_env = map { $_=>1 } split /\s+/ => $good_var_names;
my %bad_env;
for my $file (@testfiles) {
    open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
    while (<$fh>) {
        while (/\$ENV\{([^\$].*?)\}/g) {
            $bad_env{$1}++ if ! exists $valid_env{$1};
        }
    }
}

$t = q{All ENV{} calls are to known words};
%bad_env ? fail($t) : pass($t);
for my $word (sort keys %bad_env) {
    diag "Invalid ENV: $word\n";
}

$t = q{Verify the copyright year is up to date};
my $current_year = 1900 +(localtime)[5];

for my $file (qw{README Pg.pm Pg.xs Pg.h dbdimp.c dbdimp.h quote.c types.c}) {
    open $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        next unless /Copyright(.+)Greg/;
        my $years = $1;
        if ($years !~ /\b$current_year\b/) {
            fail qq{File "$file" has the wrong copyright year: expected $current_year};
        }
    }
}
pass $t;


done_testing();
