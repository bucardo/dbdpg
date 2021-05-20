#!/usr/bin/env perl

## Test combinations of Postgres for DBD::Pg
## Usage: $0 <postgresdir> [-t specific_test_file] [-c compile_version] [-r run_version] [--setup versions]

## Usage:
## Create Postgres 10,11,12,13, and 14 directories in $ENV{HOME}/pg/:
## perl dbdpg_test_postgres_versions.pl --setup 10,11,12,13,14
## Test all combinations of the same:
## perl dbdpg_test_postgres_versions.pl
## Add in the current HEAD branch, recreating if already there:
## perl dbdpg_test_postgres_versions.pl --setup head --force
## Test DBD::Pg compiled against head and run against Postgres 11:
## perl dbdpg_test_postgres_versions.pl -c head -r 11


use 5.008001;
use strict;
use warnings;
use autodie;
use Cwd;
use File::Spec::Functions;
use Getopt::Long qw/ GetOptions /;
use Data::Dumper; $Data::Dumper::Sortkeys = 1;
use Time::HiRes qw/ gettimeofday tv_interval /;
use List::Util qw/ shuffle /;

our $VERSION = 1.4;

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
   'setup=s',
);

my $testfile = $arg{testfile} || $ENV{DBDPG_TEST_FILE} || '';
my $compileversion = $arg{compileversion} || $ENV{DBDPG_COMPILE_VERSION} || '';
my $runversion = $arg{runversion} || $ENV{DBDPG_RUN_VERSION} || '';

my $basedir = shift || "$ENV{HOME}/pg";

setup_postgres_dirs() if $arg{setup};

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

    next if $compileversion and $compileversion !~ /\b$lib_version\b/;

    my $lib_dir = "$basedir/$lib_version";

    for my $target_version (shuffle @versions) {

        next if $runversion and $runversion !~ /\b$target_version\b/;

        my $target_dir = "$basedir/$target_version";

        my $readme = 'README.testdatabase';
        unlink $readme if -e $readme;

        my $outfile = "tmp/alltest.dbdpg.$lib_version.vs.$target_version.log";
        note "Testing compile $lib_version against target $target_version: results stored in $outfile";

        open my $fh, '>', $outfile;
        printf {$fh} "STARTED $lib_version vs $target_version: %s\n\n", scalar localtime;
        my $start_time = [gettimeofday];

        system "perl t/99cleanup.t >> $outfile";

        my $COM = "LD_LIBRARY_PATH=$lib_dir/lib POSTGRES_LIB= POSTGRES_INCLUDE= POSTGRES_HOME=$lib_dir perl Makefile.PL 2>&1 >> $outfile";
        note "--> $COM";
        print {$fh} "***\nRUN: $COM\n***\n\n\n";
        print {$fh} qx{$COM};

        $COM = "LD_LIBRARY_PATH=$lib_dir/lib DBDPG_TEST_ALWAYS_ENV=0 AUTHOR_TESTING=0 TEST_SIGNATURE=0 DBDPG_INITDB=$target_dir/bin/initdb make test TEST_VERBOSE=1 2>&1 >> $outfile";
        $testfile and $COM =~ s/make test/make test TEST_FILES=$testfile/;
        note "--> $COM";
        print {$fh} "***\nRUN: $COM\n***\n\n\n";
        print {$fh} qx{$COM};

        my $final_time = sprintf '%d seconds', tv_interval($start_time);
        print {$fh} "\nTIME: $final_time\n";
        close $fh;

        my $final_line = qx{tail -1 $outfile};
        chomp $final_line;
        my $date = scalar localtime;
        if ($final_line !~ /Result/) {
            $final_line = "Result: FAIL $final_line";
        }
        note "--> $final_line $lib_version vs $target_version ($date) ($final_time)\n\n";

        if ($debug_loop++ > 300) {
            die "Leaving at loop $debug_loop\n";
        }
    }
}

close $sfh;
exit;

sub setup_postgres_dirs {

    ## Create Postgres directories for one or more versions
    my $versions = $arg{setup};

    warn "Setup for version: $versions on dir $basedir\n";

    ## Must have a head
    my $giturl = 'https://github.com/postgres/postgres.git';
    my $dir = catfile($basedir, 'pg_github');
    if (-e $dir) {
        chdir $dir;
        system 'git checkout --quiet master';
        system "git pull --quiet -X theirs origin master";
    }
    else {
        system "git clone $giturl $dir";
    }
    ## Grab a list of all tags
    my $old_dir = getcwd();
    chdir($dir);
    my @taglist = qx{git tag -l};
    my %maxversion = (head => ['master','master']);
    for my $entry (@taglist) {
        chomp $entry;
        if ($entry =~ /^REL_?(\d_\d)_(\d+)$/ or $entry =~ /^REL_?(\d\d)_(\d+)$/) {
            my ($major,$revision) = ($1,$2);
            $major =~ y/_/./;
            $maxversion{$major} = [$entry,$revision] if ! exists $maxversion{$major}
                or $maxversion{$major}->[1] < $revision;
        }
    }

    for my $version (split /\s*,\s*/ => lc $arg{setup}) {
        exists $maxversion{$version} or die "Cannot find a tag for Postgres version $version\n";
        my $newdir = catfile($basedir, $version);
        my $install = 0;
        if (-e $newdir) {
            print "Directory already exists: $newdir\n";
            ## However, there may be a newer version!
            my ($existing_revision) = qx{$newdir/bin/psql --version} =~ /\.(\d+)$/;
            if ($existing_revision < $maxversion{$version}->[1]) {
                printf "For version %s, have revision %d but need %s\n",
                    $version, $existing_revision, $maxversion{$version}->[1];
                $install = 1;
            }
            else {
                print "We appear to have the latest revision: $existing_revision\n";
            }
        }
        else {
            $install = 1;
        }

        if ($install) {
            chdir($dir);
            my $tag = $maxversion{$version}->[0];
            system "git checkout $tag";
            system 'git clean -fdx';
            my $COM = "./configure --prefix=$newdir --quiet";
            if ($version =~ /^\d/ and $version <= 9.0) {
                $COM .= ' CFLAGS="-Wno-aggressive-loop-optimizations -O0"';
            }
            print "Running: $COM\n";
            system $COM;
            system 'make install';
        }
    }

    exit;

} ## end of setup_postgres_dirs
