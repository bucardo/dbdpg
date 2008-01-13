#!perl

## Run Perl::Critic against the source code and the tests
## This is highly customized, so take with a grain of salt
## Mostly useful for the core developer(s)
## Requires TEST_CRITIC to be set

use strict;
use warnings;
use Test::More;
use Data::Dumper;
select(($|=1,select(STDERR),$|=1)[1]);

if (!$ENV{TEST_CRITIC}) {
	plan skip_all => 'Set the environment variable TEST_CRITIC to enable this test';
}
elsif (!eval { require Perl::Critic; 1 }) {
	plan skip_all => 'Could not find Perl::Critic';
}
elsif ($Perl::Critic::VERSION < 0.23) {
	plan skip_all => 'Perl::Critic must be version 0.23 or higher';
}
else {
	plan tests => 1;
}

## Specific exclusions:
my %ok =
	(yaml => {
			  sub => 'meta_spec_ok',
			  },
	 pod => {
			 sub => 'pod_file_ok pod_coverage_ok',
			 },
	 signature => {
			 sub => 'verify SIGNATURE_OK',
			 },
	 Pg => {
			sub => 'foo',
			}
);


for my $f (keys %ok) {
	for my $ex (keys %{$ok{$f}}) {
		if ($ex eq 'sub') {
			for my $foo (split /\s+/ => $ok{$f}{sub}) {
				push @{$ok{$f}{OK}} => qr{Subroutine "$foo" (?:is neither|not exported)};
			}
		}
		else {
			die "Unknown exception '$ex'\n";
		}
	}
}

## Check the non-test files
my $critic = Perl::Critic->new(-severity => 4, '-profile-strictness', 'quiet');

for my $filename (qw/Pg.pm/) {
	-e $filename or die qq{Could not find $filename\n};
	diag "Running Perl::Critic on $filename...\n";
	my @bad = $critic->critique($filename);
	my $baditems = 0;
  VIO: for my $v (@bad) {
		my $d = $v->description();
		my $f = $v->filename();
		next if $d =~ /Subroutine "SQL_\w+" (?:not exported|is neither)/;
		next if $d =~ /Subroutine "pg_\w+" not exported/;
		next if $d =~ /Subroutine "looks_like_number" not exported/;
		for my $k (sort keys %ok) {
			next unless $f =~ /$k/;
			for (@{$ok{$k}{OK}}) {
				next VIO if $d =~ $_;
			}
		}
		$baditems++;
		my $l = $v->location();
		my $line = $l->[0];
		my $policy = $v->policy();
		my $source = $v->source();
		diag "$d ($f: $line)\n";
		diag "[-$policy]\n";
		diag "S=$source\n\n";
	}
}

$critic = Perl::Critic->new(-severity => 1, -theme => 'core');

## Allow Test::More subroutines
my $tm = join '|' => (qw/skip plan pass fail is ok diag BAIL_OUT/);
my $testmoreok = qr{Subroutine "$tm" is neither};

opendir my $dir, 't' or die qq{Could not open directory 't': $!\n};
my @files = map { "t/$_" } grep { /\.t$/ } readdir $dir;
closedir $dir;

for my $filename (@files) {
	diag "Running Perl::Critic on $filename...\n";
	my @bad = $critic->critique($filename);
	my $baditems = 0;
  VIO: for my $v (@bad) {
		my $d = $v->description();
		my $f = $v->filename();
		next if $d =~ $testmoreok;
		for my $k (sort keys %ok) {
			next unless $f =~ /$k/;
			for (@{$ok{$k}{OK}}) {
				next VIO if $d =~ $_;
			}
		}
		$baditems++;
		my $l = $v->location();
		my $line = $l->[0];
		my $policy = $v->policy();
		my $source = $v->source();
		diag "$d ($f: $line)\n";
		diag "[-$policy]\n";
		diag "S=$source\n\n";
	}
}

pass('Finished Perl::Critic testing');

