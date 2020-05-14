#!perl

## Minor code cleanup checks

use 5.008001;
use strict;
use warnings;
use Test::More;
use File::Find;

my (@testfiles,@perlfiles,@cfiles,%fileslurp,$t);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
}

$ENV{LANG} = 'C';
find (sub { push @cfiles    => $File::Find::name if /\.c$/ and $_ ne 'Pg.c'; }, '.');
find (sub { push @testfiles => $File::Find::name if /^[^.]\w+\.(t|pl)$/; }, 't');
find (sub { push @perlfiles => $File::Find::name if /\.(pm|pl|t)$/; }, '.');

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

##
## Everything in MANIFEST[.SKIP] should also be in README.dev
##
for my $file (sort keys %manfile) {
    if (!exists $devfile{$file}) {
        fail qq{File "$file" is in MANIFEST but not in README.dev\n};
    }
}

##
## Everything in README.dev should also be in MANIFEST, except derived files
##
my %derived = map { $_, 1 } qw/Makefile Pg.c README.testdatabase dbdpg_test_database/;
for my $file (sort keys %devfile) {
    if (!exists $manfile{$file} and !exists $derived{$file}) {
        fail qq{File "$file" is in README.dev but not in MANIFEST\n};
    }
    if (exists $manfile{$file} and exists $derived{$file}) {
        fail qq{File "$file" is derived and should not be in MANIFEST\n};
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
            next if $line =~ /\w+ fail/;
            next if $line =~ /defined \$expected \? like/;
            like ($line, qr{^\s*#?$func \(['\S]}, $t);
        }
    }
}

##
## Check C and Perl files for errant tabs
##
for my $file (@cfiles, @perlfiles) {
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
    t/03dbmethod.t t/03smethod.t t/12placeholders.t t/01constants.t t/99_yaml.t
    testme.tmp.pl
}) {
    open $fh, '<', $file or die "Could not open $file: $!\n";
    while (<$fh>) {
        s!([A-Za-z][A-Za-z']+)!(my $x = $1) =~ s/'$//; delete $word{$x}; ' '!ge;
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


done_testing();
