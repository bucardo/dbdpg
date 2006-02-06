#!perl -w

use Test::More;
use strict;

# Check our Pod
# The top test was provided by Andy Lester, who stole it from Brian D. Foy
# Thanks to both !

plan tests => 3;

my $PODVERSION = '0.95';
eval {
	require Test::Pod;
	Test::Pod->import;
};
if ($@ or $Test::Pod::VERSION < $PODVERSION) {
	pass("Skipping Test::Pod testing") for (1..2);
}
else {
	# We defer loading these until we know Test::Pod is ready
 	require File::Find;
	require File::Spec;
	File::Find->import;
	File::Spec->import;
	my $blib = File::Spec->catfile(qw(blib lib));
	my @files;
	find( sub { 
			# The 'defined' test is just to avoid compiler warnings
			push @files, $File::Find::name if /\.p(l|m|od)$/o and defined $File::Find::name;
	}, $blib);
	foreach my $file (@files) {
		pod_file_ok($file);
	}
}

## We won't require everyone to have this, so silently move on if not found
my $PODCOVERVERSION = '1.04';
eval {
	require Test::Pod::Coverage;
	Test::Pod::Coverage->import;
};
if ($@ or $Test::Pod::Coverage::VERSION < $PODCOVERVERSION) {
	pass ("DBD::Pg pod coverage skipped");
}
else {
	my $trusted_names  = 
		[
		 qr{^PG_[A-Z]+\d?$},
		 qr{^CLONE$},
		 qr{^driver$},
		 qr{^constant$},
		];
	pod_coverage_ok("DBD::Pg", {trustme => $trusted_names}, "DBD::Pg pod coverage okay");
}
