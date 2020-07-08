#!/usr/bin/env perl

## Quick script to test all available combinations of Postgres
## Usage: $0 <postgresdir> [-t specific_test_file] [-c compile_version] [-r run_version]

use 5.008001;
use strict;
use warnings;
use autodie;
use Getopt::Long qw/ GetOptions /;
use Data::Dumper;
use Time::HiRes qw/ gettimeofday tv_interval /;
use List::Util qw/ shuffle /;

our $VERSION = 1.1;

my %arg = (
    quiet => 0,
);

GetOptions
 (
     \%arg,
   'verbose',
   'quiet',
   'testfile=s',
   'compileversion=s',
   'runversion=s',
   'wipe',
);

my $testfile = $arg{testfile} || $ENV{DBDPG_TEST_FILE} || '';
my $compileversion = $arg{compileversion} || $ENV{DBDPG_COMPILE_VERSION} || '';
my $runversion = $arg{runversion} || $ENV{DBDPG_RUN_VERSION} || '';

my $basedir = shift || "$ENV{HOME}/pg";

my $dh;
opendir $dh, $basedir;
my @versions = grep { /^[1-9][\.0-9]+$/ or /^head$/i } readdir $dh;
closedir $dh;

## Sanity check:
for my $lver (@versions) {
    my $libdir = "$basedir/$lver/lib";
    -d $libdir or die qq{Could not find directory: $libdir\n};
}

if ($arg{wipe}) {
    opendir $dh, 'tmp';
    for my $file (grep { /^alltest\.dbdpg.+\.log$/ } readdir $dh) {
        unlink "tmp/$file";
    }
}

my $summaryfile = 'tmp/summary.testallversions.log';
open my $sfh, ($arg{wipe} ? '>' : '>>'), $summaryfile;
printf {$sfh} "\nSTARTED $0 at %s\n\n", scalar localtime;

sub note {
    my $message = shift or die;
    chomp $message;
    $arg{quiet} or print "$message\n";
    print {$sfh} "$message\n";
    return;
}

my $debug_loop = 0;
for my $lib_version (shuffle @versions) {

    next if $compileversion and $lib_version ne $compileversion;

    my $lib_dir = "$basedir/$lib_version";

    for my $target_version (shuffle @versions) {

        next if $runversion and $target_version ne $runversion;

        my $target_dir = "$basedir/$target_version";

        unlink 'README.testdatabase';

        my $outfile = "tmp/alltest.dbdpg.$lib_version.vs.$target_version.log";
        note "Testing compile $lib_version against target $target_version: results stored in $outfile";

        open my $fh, '>', $outfile;
        printf {$fh} "STARTED $lib_version vs $target_version: %s\n\n", scalar localtime;
        my $start_time = [gettimeofday];

        system "perl t/99cleanup.t >> $outfile";

        my $COM = "POSTGRES_LIB= POSTGRES_INCLUDE= POSTGRES_HOME=$lib_dir perl Makefile.PL 2>&1 >> $outfile";
        note "--> $COM";
        print {$fh} "***\nRUN: $COM\n***\n\n\n";
        print {$fh} qx{$COM};

        $COM = "DBDPG_TEST_ALWAYS_ENV=0 AUTHOR_TESTING=0 TEST_SIGNATURE=0 DBDPG_INITDB=$target_dir/bin/initdb make test TEST_VERBOSE=1 2>&1 >> $outfile";
        $testfile and $COM =~ s/make test/make test TEST_FILES=$testfile/;
        note "--> $COM";
        print {$fh} "***\nRUN: $COM\n***\n\n\n";
        print {$fh} qx{$COM};

        my $final_time = tv_interval($start_time);
        note "Time: $final_time";
        print {$fh} "\nTIME: $final_time\n";
        close $fh;

        my $final_line = qx{tail -1 $outfile};
        chomp $final_line;
        note "--> $final_line $lib_version vs $target_version\n\n";

        if ($debug_loop++ > 300) {
            die "Leaving at loop $debug_loop\n";
        }
    }
}

close $sfh;
exit;
