#!perl -w

use Test::More;
use strict;

# Check our Pod
# The test was provided by Andy Lester, who stole it from Brian D. Foy
# Thanks to both !

my $PODVERSION = '0.95';
eval {
	require Test::Pod;
	Test::Pod->import;
};
if ($@ or $Test::Pod::VERSION < $PODVERSION) {
	plan skip_all => "Test::Pod $PODVERSION required for testing POD";
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
	plan tests => scalar @files;
	foreach my $file (@files) {
		pod_file_ok($file);
	}
}
