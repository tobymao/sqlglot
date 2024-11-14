# Contributing to [SQLGlot](https://github.com/tobymao/sqlglot/blob/main/README.md)

SQLGLot is open source software. We value feedback and we want to make contributing to this project as
easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features

## We develop with Github

We use github to host code, to track issues and feature requests, as well as accept pull requests.

## Finding tasks to work on

When the core SQLGlot team does not plan to work on an issue, it is usually closed as "not planned". This may happen
when a request is exceptionally difficult to address, or because the team deems that it shouldn't be prioritized.

These issues can be a good starting point when looking for tasks to work on. Simply filter the issue list to fetch
the closed issues and then search for those marked as "not planned". If the scope of an issue is not clear or you
need guidance, feel free to ask for clarifications.

Before taking on a task, consider studying the [AST primer](https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md) and the [onboarding document](https://github.com/tobymao/sqlglot/blob/main/posts/onboarding.md).

## Submitting code changes

Pull requests are the best way to propose changes to the codebase, and we actively welcome them.

Pull requests should be small and they need to follow the conventions of the project. For features that require
many changes, please reach out to us on [Slack](https://tobikodata.com/slack) before making a request, in order
to share any relevant context and increase its chances of getting merged.

1. Fork the repo and create your branch from `main`
2. If you've added code with non-trivial changes, add tests
3. If you've changed APIs, update the documentation (docstrings)
4. Ensure the test suite & linter [checks](https://github.com/tobymao/sqlglot/blob/main/README.md#run-tests-and-lint) pass
5. Issue that pull request and wait for it to be reviewed by a maintainer or contributor

Note: make sure to follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) guidelines when creating a PR.

## Report bugs using Github's [issues](https://github.com/tobymao/sqlglot/issues)

We use GitHub issues to track public bugs. Report a bug by opening a new issue.

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific
  - Give sample code if you can
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)
- References (e.g. documentation pages related to the issue)

## Start a discussion using Github's [discussions](https://github.com/tobymao/sqlglot/discussions)

[We use GitHub discussions](https://github.com/tobymao/sqlglot/discussions/190) to discuss about the current state
of the code. If you want to propose a new feature, this is the right place to do it. Just start a discussion, and
let us know why you think this feature would be a good addition to SQLGlot (by possibly including some usage examples).

## [License](https://github.com/tobymao/sqlglot/blob/main/LICENSE)

By contributing, you agree that your contributions will be licensed under its MIT License.

## References

This document was adapted from [briandk's template](https://gist.github.com/briandk/3d2e8b3ec8daf5a27a62).
