#!perl -w

use Test::More;
use strict;

# Check our Pod

plan tests => 2;

my $PODVERSION = '0.95';
eval {
	require Test::Pod;
	Test::Pod->import;
};
if ($@ or $Test::Pod::VERSION < $PODVERSION) {
	pass("Skipping Test::Pod testing");
}
else {
	pod_file_ok("Pg.pm");
}

## We won't require everyone to have this, so silently move on if not found
my $PODCOVERVERSION = '1.04';
eval {
	require Test::Pod::Coverage;
	Test::Pod::Coverage->import;
};
if ($@ or $Test::Pod::Coverage::VERSION < $PODCOVERVERSION) {
	pass ("Skipping Test::Pod::Coverage testing");
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
