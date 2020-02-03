#!/usr/bin/env perl

## Quick script to test all available combinations of Postgres
## Usage: $0 <postgresdir>

use 5.008001;
use strict;
use warnings;

my $basedir = shift || "$ENV{HOME}/code/postgres";

-d $basedir or die qq{No such directory: $basedir\n};

my @versions = qw/ 9.0 9.1 9.2 9.3 HEAD /;

## Sanity check:
for my $lver (@versions) {
    my $libdir = "$basedir/$lver/lib";
    -d $libdir or warn qq{Could not find directory: $libdir\n};
}

for my $lver (@versions) {
    my $libdir = "$basedir/$lver/lib";
    next if ! -d $libdir;
    for my $tver (@versions) {

        my $libdir2 = "$basedir/$tver/lib";
        next if ! -d $libdir2;

        my $outfile = "dbdpg.testing.$lver.vs.$tver.log";
        print "Testing library $lver against $tver: results stored in $outfile\n";
        open my $fh, '>', $outfile or die qq{Could not open "$outfile": $!\n};

        my $COM = "POSTGRES_LIB=/home/greg/code/postgres/$lver/lib perl Makefile.PL 2>&1";
        print "$COM\n";
        print $fh qx{$COM};

        (my $port = $tver) =~ s/\.//;
        $port = "5$port" . 0;
        $port =~ /HEAD/ and $port = 5999;

        $COM = "PGPORT=$port make test";
        print "$COM\n";
        print $fh qx{$COM};
    }
}
