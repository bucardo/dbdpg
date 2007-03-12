#!perl -w

use Test::More;
use strict;

plan tests => 2;

my $V = 0.03;
eval {
	require Test::YAML::Meta;
	Test::YAML::Meta->import;
};
if ($@) {
	SKIP: {
		skip "Skipping Test::YAML::Meta tests: module not found", 2;
	}
}
elsif ($Test::YAML::Meta::VERSION < $V) {
	SKIP: {
		skip "Skipping Test::YAML::Meta tests: need version $V, but only have $Test::YAML::Meta::VERSION", 2;
	}
}
else {
	meta_spec_ok('META.yml', 1.3);
}
