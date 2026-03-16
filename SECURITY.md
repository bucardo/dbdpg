This is the Security Policy for the Perl DBD-Pg distribution.

Report security issues via the
[private security issue reporting feature in GitHub](https://github.com/bucardo/dbdpg/security/advisories/new).

The latest version of the Security Policy can be found in the
[GitHub repository for DBD::Pg](https://github.com/bucardo/dbdpg).

This text is based on the CPAN Security Group's Guidelines for Adding
a Security Policy to Perl Distributions (version 1.4.2)
https://security.metacpan.org/docs/guides/security-policy-for-authors.html

# How to Report a Security Vulnerability

Security vulnerabilities can be reported via the
[private security issue reporting feature in GitHub](https://github.com/bucardo/dbdpg/security/advisories/new).

Please include as many details as possible, including code samples
and/or test cases, so that the issue can be reproduced.  Check that
your report does not expose any sensitive data, such as passwords,
tokens, or personal information.

Project maintainers will normally credit the reporter when a
vulnerability is disclosed or fixed.  If you do not want to be
credited publicly, please indicate that in your report.

If you would like any help with triaging the issue, or if the issue
is being actively exploited, please copy the report to the CPAN
Security Group (CPANSec) at <cpan-security@security.metacpan.org>.

Please *do not* use the public issue reporting system on RT or
GitHub issues for reporting security vulnerabilities in DBD::Pg.

Please do not disclose the security vulnerability in public forums
until past any proposed date for public disclosure, or it has been
made public by the maintainers or CPANSec.  That includes patches or
pull requests or mitigation advice.

For more information, see
[Report a Security Issue](https://security.metacpan.org/docs/report.html)
on the CPANSec website.

## Response to Reports

The maintainer(s) aim to acknowledge your security report as soon as
possible.  However, they cannot guarantee a rapid response.  If you
have not received a response from them within a week, then
please send a reminder to them and copy the report to CPANSec at
<cpan-security@security.metacpan.org>.

Please note that the initial response to your report will be an
acknowledgement, with a possible query for more information.  It
will not necessarily include any fixes for the issue.

The project maintainer(s) may forward this issue to the security
contacts for other projects where we believe it is relevant.  This
may include embedded libraries, system libraries, prerequisite
modules or downstream software that uses this software.

They may also forward this issue to CPANSec.

# Which Software This Policy Applies To

Any security vulnerabilities in DBD::Pg are covered by this policy.

Security vulnerabilities in versions of any libraries that are
included in DBD::Pg are also covered by this policy.

Security vulnerabilities are considered anything that allows users
to execute unauthorised code, access unauthorised resources, or to
have an adverse impact on accessibility, integrity or performance of
a system.

Security vulnerabilities in upstream software (prerequisite modules
or system libraries, or in Perl), are not covered by this policy
unless they affect DBD::Pg, or DBD::Pg can be used to exploit
vulnerabilities in them.

Security vulnerabilities in downstream software (any software that
uses DBD::Pg, or plugins to it that are not included with the
DBD::Pg distribution) are not covered by this policy.

## Supported Versions of DBD::Pg

The maintainer(s) will release security fixes for the latest version
of DBD::Pg only.

If a security vulnerability can be fixed by increasing the minimum
version of Perl or the minimum version of other third-party software
prerequisite, then they may do so.

# Installation and Usage Issues

The distribution metadata specifies minimum versions of
prerequisites that are required for DBD::Pg to work.  However, some
of these prerequisites may have security vulnerabilities, and you
should ensure that you are using the most up-to-date versions of these
prerequisites when assessing security vulnerabilities in DBD::Pg.

Where security vulnerabilities are known, the metadata may indicate
newer versions as recommended.
