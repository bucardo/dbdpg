#!perl

## Make sure the version number is consistent in all places
## Check on the format of the Changes file

use 5.010;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Data::Dumper;
use Test::More;
use feature 'unicode_strings';

if (! $ENV{RELEASE_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable RELEASE_TESTING is set');
}
plan tests => 3;

my $vre = qr{([0-9]+\.[0-9]+\.[0-9]+\_?[0-9]*)};

my %filelist = (
    'dbdimp.c'             => [1, [ qr{ping test v$vre},        ]],
    'META.yml'             => [3, [ qr{version\s*:\s*$vre},     ]],
    'META.json'            => [3, [ qr{"version" : "$vre"},     ]],
    'Pg.pm'                => [4, [ qr{VERSION = qv\('$vre'},
                                    qr{realversion = qv\('$vre'},
                                    qr{documents version $vre},
                                    qr{ping test v$vre},        ]],
    'lib/Bundle/DBD/Pg.pm' => [1, [ qr{VERSION = '$vre'},       ]],
    'Makefile.PL'          => [1, [ qr{VERSION = '$vre'},       ]],
    'README'               => [1, [ qr{is version $vre},
                                    qr{TEST VERSION \($vre},    ]],
    'Changes'              => [1, [ qr{^(?:Version )*$vre},     ]],
);

my %v;
my $goodversion = 1;
my $goodcopies = 1;
my $lastversion = '?';

## Walk through each file and slurp out the version numbers
## Make sure that the version number matches
## Verify the total number of version instances in each file as well

for my $file (sort keys %filelist) {
    my ($expected,$regexlist) = @{ $filelist{$file} };

    my $instances = 0;
    open my $fh, '<', $file or die qq{Could not open "$file": $!\n};
  SLURP: while (<$fh>) {
        for my $regex (@{ $regexlist }) {
            if (/$regex/) {
                my $foundversion = $1;
                push @{$v{$file}} => [$foundversion, $.];
                if ($lastversion =~ /[0-9]/ and $foundversion ne $lastversion) {
                    $goodversion = 0;
                }
                $lastversion = $foundversion;
                $instances++;
                last SLURP if $file eq 'Changes'; ## Only the top version please
            }
        }
    }
    close $fh or warn qq{Could not close "$file": $!\n};

    if ($file eq 'README' and $lastversion =~ /_/) {
        ## Beta gets two mentions in README
        $expected++;
    }

    if ($instances != $expected) {
        $goodcopies = 0;
        diag "Version instance mismatch for $file: expected $expected, found $instances";
    }

}


if ($goodcopies) {
    pass ('All files had the expected number of version strings');
}
else {
    fail ('All files did not have the expected number of version strings');
}

if ($goodversion) {
    pass ("All version numbers are the same ($lastversion)");
}
else {
    fail ('All version numbers were not the same!');
    for my $filename (sort keys %v) {
        for my $glob (@{$v{$filename}}) {
            my ($ver,$line) = @$glob;
            diag "File: $filename. Line: $line. Version: $ver\n";
        }
    }
}

my $changes_file_ok = 1;
open my $fh, '<', 'Changes' or die "Could not find the 'Changes' file\n";
my $month = '(January|February|March|April|May|June|July|August|September|October|November|December)'; ## no critic (Variables::ProhibitUnusedVarsStricter)
my ($lastline1, $lastline2, $lastline3) = ('','','');
my $seen_a_version = 0;
while (<$fh>) {
    chomp;
    if (/\bVersion/) {
        next if /unreleased/;
        next if ! $seen_a_version++;

        if ($lastline1 =~ /\w/ or $lastline2 =~ /\w/ or $lastline3 !~ /\w/) {
            diag "Changes file fails double spacing before: $_\n";
            $changes_file_ok = 0;
        }

        if (! /^Version [0-9]\.[0-9][\.0-9]* /) {
            diag "Changes file version failure: $_\n";
            $changes_file_ok = 0;
        }
        if (! /^Version [0-9]\.[0-9][\.0-9]*  \S/) {
            diag "Changes file spacing failure: $_\n";
            $changes_file_ok = 0;
        }
        if (! /^Version [0-9]\.[0-9][\.0-9]*  \(released $month [0-9][0-9]*, [0-9][0-9][0-9][0-9]\)$/) {
            diag "Changes file release date failure: $_\n";
            $changes_file_ok = 0;
        }
    }
    if (/\w/ and $lastline1 =~ /^Version ([0-9].[0-9][\.0-9]+)/) {
        diag "Changes file does not have space after version $1\n";
        $changes_file_ok = 0;
    }
    $lastline3 = $lastline2;
    $lastline2 = $lastline1;
    $lastline1 = $_;
}

## Check for standardized entries
seek $fh, 0, 0;
$. = 0;
while (<$fh>) {
    chomp;
    next if ! /\w/;
    next if /^RT refers to/;
    next if /^Version [0-9]+\.[0-9]+/;
    next if /^Version \?\?/;
    next if /SYSTEM VIEW/;
    next if /^Changes for/;

    my $extra = '(?: and other places)*';

    ## RT tickets - three spaces, parens
    next if /^   \(RT ticket \#[0-9]+$extra\)$/;
    ## Two tickets
    next if /^   \(RT tickets \#[0-9]+ and \#[0-9]+\)$/;
    ## Three or more tickets
    next if /^   \(RT tickets \#[0-9]+(?:[, \#0-9])*\)$/;

    ## Github issues and pull requests - three spaces, parens
    next if /^   \(Github (?:issue|pull request) #[0-9]+\)$/;

    ## Debian issue
    next if /^   \(Debian bug \#[0-9]+\)$/;

    ## Should not have any bug tracking keywords now
    if (/RT/ or /github/i or /cpan/i or /debian /i) {
        ## Allow a few exceptions
        if (! /dbdpg.git/ and ! /META/ and ! /cpan\.org/ and ! /Github user/) {
            fail ("Found mention of bug tracker at wrong place at line $.: $_\n");
            next;
        }
    }

    ## Authors - three spaces, bracketed names
    next if /^   \[[A-Z][\w\. \P{IsLower}]+\]$/;

    ## Authors - three spaces, email
    next if /^   \[\w[\w\.\-]+ at \w+[\w \.]+\]$/;

    ## Spotted by
    next if /^   \[spotted by .*?\]$/;

    if (/\[(?![x0-9])/ or /(?=[y0-9])\]/) {
        fail ("Found a brace at line $.: $_\n");
        next;
    }

    ## Start of an item
    next if /^ \- [A-Z]\w+/;

    ## Continuation line - new sentence
    next if /^   [A-Z][A-Za-z]/; ## Do not want to match RT or CPAN though

    ## Continuation line - same sentence
    next if /^     [a-z]/;

    die "Unknown line at $. of Changes file: $_\n";

}


close $fh;

if ($changes_file_ok) {
    pass (q{The 'Changes' file is in the correct format});
}
else {
    fail (q{The 'Changes' file does not have the correct format});
}



exit;

