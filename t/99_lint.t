#!perl

## Various code cleanup checks

use 5.008001;
use strict;
use warnings;
use Test::More;

my (@testfiles,@perlfiles,@cfiles,@headerfiles,%fileslurp,$t);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
}

$ENV{LANG} = 'C';

my ($all_ok, $count);

my $file = 'MANIFEST';
if (! -e $file) {
    plan (skip_all => "Could not find the $file file");
    exit;
}

open my $fh, '<', $file or die "Could not open $file: $!\n";
@perlfiles = map { chomp; $_ } grep { /\.pm$/ or /\.t$/ } <$fh>;
seek $fh ,0, 0;
@testfiles = map { chomp; $_ } grep { m{t/.*\.t$} } <$fh>;
seek $fh ,0, 0;
@headerfiles = map { chomp; $_ } grep { m{\.h$} } <$fh>;
seek $fh ,0, 0;
@cfiles = map { chomp; $_ } grep { m{\.c$} } <$fh>;
close $fh;

## A few things are in MANIFEST.SKIP but we still want to check on them
$file = 'MANIFEST.SKIP';
if (! -e $file) {
    plan (skip_all => "Could not find the $file file");
    exit;
}
open $fh, '<', $file or die "Could not open $file: $!\n";
push @testfiles => map { chomp; $_ } grep { m{t/.*\.t} } <$fh>;
seek $fh, 0, 0;
push @perlfiles => map { chomp; $_ } grep { m{t/.*\.t} } <$fh>;
close $fh;

##
## Ensure all test names are unique
##
my %message;
for my $file (@testfiles) {
    open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
    while (my $line = <$fh>) {
        chomp $line;
        last if $line =~ /__DATA__/; ## perlcritic.t
        next if $line !~ /^\s*\$t\b/;
        next if $line !~ s/^\s*\$t\s*=\s*//;

        $line =~ s/^'(.+)'.*/$1/;
        $line =~ s/^"(.+)".*/$1/;
        $line =~ s/^q\{(.+)\}.*/$1/;
        $line =~ s/^qq\{(.+)\}.*/$1/;

        ## Assume anything with a variable knows what it is doing
        next if $line =~ /\$/;

        if (exists $message{$line}) {
            my $msg = sprintf 'Duplicated test name in file %s:%d:%d >>%s<<',
                $file, $message{$line}, $., $line;
            fail $msg;
        }
        $message{$line} = $.;

    }
    close $fh or die qq{Could not close "$file": $!\n};
}


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

##
## Make sure the README.dev mentions all files used, and jives with the MANIFEST
##
$file = 'README.dev';
open $fh, '<', $file or die qq{Could not open "$file": $!\n};
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
$all_ok = 1;
$count = 0;
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
        ## Specific exceptions
        next if /ok to not trace/;

        while (/\b(PQ[a-z]\w+)\(/g) {
            my $func = $1;
            ## Items small and rare enough not to trace:
            next if $func eq 'PQgetlength';
            next if $func eq 'PQgetvalue';
            next if $func eq 'PQbinaryTuples';
            next if $func eq 'PQflush';

            $count++;
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
    if (!$traceok) {
        fail (qq{File "$file" does not have TRACE calls for each PQ function});
        $all_ok = 0;
    }
}
if ($all_ok) {
    pass (qq{Scanned $count locations; all PQ functions have matching TRACE calls});
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
my %derived = map { $_, 1 } qw/Makefile Pg.c README.testdatabase dbdpg_test_database testdb dbdpg_test_postgres_versions.pl/;
for my $file (sort keys %devfile) {
    next if $file =~ m{misc/};
    if (!exists $manfile{$file} and !exists $derived{$file}) {
        fail qq{File "$file" is in README.dev but not in MANIFEST\n};
    }
    if (exists $manfile{$file} and exists $derived{$file}) {
        fail qq{File "$file" is derived and should not be in MANIFEST\n};
    }
}

## All files in the repo should be mentioned in the README.dev
SKIP: {

    skip 'Cannot verify repo contents when using Windows', 1 if $^O =~ /Win32/;

    my %gitfiles = map { chomp; $_, 1 } qx{git -c 'safe.directory=*' ls-files};
    for my $file (sort keys %gitfiles) {
        next if $file =~ /^z_announcements/;
        next if $file =~ /^\.git/;
        if (!exists $devfile{$file}) {
            fail qq{File "$file" is in the repo but not in the README.dev file};
        }
    }
}

##
## Make sure all Test::More function calls are standardized
##
$all_ok = 1;
$count = 0;
for my $file (sort keys %fileslurp) {
    for my $linenum (sort {$a <=> $b} keys %{$fileslurp{$file}}) {
        for my $func (sort keys %{$fileslurp{$file}{$linenum}}) {
            $count++;
            $t=qq{Test::More method "$func" is in standard format inside $file at line $linenum};
            my $line = $fileslurp{$file}{$linenum}{$func};
            ## Must be at start of line (optional whitespace and comment), a space, a paren, and something interesting
            next if $line =~ /testmorewords/;
            next if $line =~ /\w+ fail/;
            next if $line =~ /defined \$expected \? like/;
            if ($line !~ qr{^\s*#?$func \(['\S]}) {
                fail ($t);
                $all_ok = 0;
            }
        }
    }
}
if ($all_ok) {
    pass ("Scanned $count test lines; all have correct format");
}

##
## Check C and Perl files for errant tabs
##
$all_ok = 1;
$count = 0;
for my $file (@cfiles, @headerfiles, @perlfiles) {

    ## This one is special, and out of our control:
    next if $file eq 'dbivport.h';

    $count++;
    my $tabfail = 0;
    open my $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        $tabfail++ if /\t/;
    }
    close $fh;
    if ($tabfail) {
        fail (qq{File "$file" contains one or more tabs: $tabfail});
        $all_ok = 0;
    }
}
if ($all_ok) {
    pass ("Scanned $count files; none have tabs");
}

##
## Check files for trailing spaces
##
$all_ok = 1;
$count = 0;
for my $file (@cfiles, @headerfiles, @perlfiles) {
    my $spacefail = 0;
    $count++;
    open my $fh, '<', $file or die "Could not open $file: $!\n";
    my $lastline = 0;
    while (<$fh>) {
        if (/ $/) {
            $spacefail++;
            $lastline = $.;
            diag "found at $.";
        }
    }
    close $fh;
    if ($spacefail) {
        fail (qq{File "$file" has this many lines with trailing spaces: $spacefail (last at $lastline)});
        $all_ok = 0;
    }
}

if ($all_ok) {
    pass ("Scanned $count files; none have trailing spaces");
}


##
## Make sure all Perl files request the same minimum version of Perl
##
my $canonical_version = 5.008001;
my %ver;
for my $file (@perlfiles) {

    ## The App::Info items do not need this check
    next if $file =~ m{/App/Info};

    open my $fh, '<', $file or die "Could not open $file: $!\n";
    my $minversion = 0;
    while (<$fh>) {
        if (/^use ([0-9]+\.[0-9]+);$/) {
            $ver{$file} = $1;
            last;
        }
    }

    close $fh;
    if (! exists $ver{$file}) {
        fail (qq{Failed to find a minimum Perl version for the file $file});
    }
}

$all_ok = 1;
$count = 0;
for my $file (sort keys %ver) {
    my $version = $ver{$file};
    $count++;
    if ($version ne $canonical_version) {
        fail(qq{Wrong minimum Perl version ($version is not $canonical_version) for file $file});
        $all_ok = 0;
    }
}
if ($all_ok) {
    pass(qq{Scanned $count Perl files; all require version $canonical_version});
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
    t/04misc.t t/05leak.t t/12placeholders.t t/99_yaml.t
    testme.tmp.pl dbdpg_test_postgres_versions.pl
}) {
    open $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        s{([A-Za-z][A-Za-z']+)}{(my $x = $1) =~ s/'$//; delete $word{$x}; ' '}ge;
    }
}

## Special case for words only found in comments
for (qw/ issuecomment /) {
    delete $word{$_};
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
DBI_DSN DBI_USER DBI_PASS DBI_DRIVER
DBDPG_INITDB DBDPG_DEBUG DBDPG_NOCLEANUP DBDPG_TESTINITDB DBDPG_TEST_ALWAYS_ENV DBDPG_TEST_NOHELPFILE DBDPG_TEMPDIR
DBDPG_TEST_LOCALE
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

##
## Verify that result_shared is always set properly when last_result is set
##

$all_ok = 1;
$file = 'dbdimp.c';
open $fh, '<', $file or die "Could not open $file: $!\n";
$.=0;
my $pointers = 0;
while (<$fh>) {
    chomp;

    if ($pointers) {
        if (1 == $pointers) {
            $t = "Incorrect assignment of result_shared to false at $file line $.";
            if (! / imp_dbh->result_shared = DBDPG_FALSE;/) {
                fail($t);
                $all_ok = 0;
            }
        }
        elsif (2 == $pointers) {
            $t = "Incorrect assignment of result_shared to true at $file line $.";
            if (! / imp_dbh->result_shared = DBDPG_TRUE;/) {
                fail($t);
                $all_ok = 0;
            }
        }
        else {
            fail "Cannot handle more than two assignments for $file line $.";
            $all_ok = 0;
        }
        $pointers = 0;
    }

    ## Find lines in which we are setting last_result to something
    next if ! /last_result\s*=\s*(.+)/;

    # Skip places where we simply are setting it to null
    next if /=\s*NULL/;

    ## How many things are we assigning to this? (expect one or two only)
    $pointers = (tr/=//);
    $pointers or die "Expected an assignment for $_\n";

    ## If this is a single-line statement, we continue
    next if /;$/;

    ## Otherwise, go until we get a semi-colon
    1 while <$fh> !~ /;/;
}

if ($all_ok) {
    pass('All calls to last_result in dbdimp.c have correct result_shared');
}

##
## All calls to Safefree must immediately set to null if something else might try and read it
##

$all_ok = 1;
$file = 'dbdimp.c';
open $fh, '<', $file or die "Could not open $file: $!\n";
$.=0;
my $current_function = '?';
while (<$fh>) {
    chomp;

    ## /* Get the current function */
    if (/^\w.+ (\w+) ?\(/) {
        $current_function = $1;
        next;
    }

    if (/->/ and /Safefree\((.+)\)/) {

        my $var = $1;

        ## These are safe and controlled
        next if $var =~ /elem->/;

        my $nextline = <$fh>;

        ## We want to immediately NULL it
        next if $nextline =~ /$var = NULL/;

        ## Also okay of we use New() or Newx()
        next if $nextline =~ /New.+$var/;

        ## Special case for dbd_st_destroy
        next if $current_function eq 'dbd_st_destroy';

        fail "Found suspicious Safefree() on $var at line $. ($current_function)";
        $all_ok = 0;

    }
}

if ($all_ok) {
    pass('All Safefree calls in dbdimp.c look ok');
}



done_testing();
