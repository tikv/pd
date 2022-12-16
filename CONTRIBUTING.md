# How to contribute

Thanks for your interest in contributing to PD!

## Finding something to work on

For beginners, we have prepared many suitable tasks for you. Checkout our [Help Wanted issues](https://github.com/tikv/pd/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22) list, in which we have also marked the difficulty level.

If you are planning something big, for example, relates to multiple components or changes current behaviors, make sure to open an issue to discuss with us before going on.

## Getting started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, and submit patches!

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create an issue about the problem that you are going to solve if there isn't existed (see below for [linking issues](#linking-issues)).
- Create a topic branch from where you want to base your work. This is usually master.
- Make commits of logical units and add test cases if the change fixes a bug or adds new functionality.
- Run tests and make sure all the tests are passed.
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request and make sure proper final commit message (see below for [message format](#format-of-the-commit-message)).
- Your PR must receive LGTMs from two reviewers.

More specifics on the development workflow are in [development workflow](./docs/development-workflow.md).

More specifics on the coding flow are in [development](./docs/development.md).

Thanks for your contributions!

### Code style

The coding style suggested by the Golang community is used in PD. See the [style doc](https://github.com/golang/go/wiki/CodeReviewComments) for details.

Please follow this style to make PD easy to review, maintain and develop.

### Linking issues

Code repositories in TiKV community require **ALL** the pull requests referring to their corresponding issues. In the pull request body, there **MUST** be one line starting with `Issue Number: ` and linking the relevant issues via the [keyword](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword), for example:

If the pull request resolves the relevant issues, and you want GitHub to close these issues automatically after it merged into the default branch, you can use the syntax (`KEYWORD #ISSUE-NUMBER`) like this:

```
Issue Number: close #123
```

If the pull request links an issue but does not close it, you can use the keyword `ref` like this:

```
Issue Number: ref #456
```

Multiple issues should use full syntax for each issue and separate by a comma, like:

```
Issue Number: close #123, ref #456
```

For pull requests trying to close issues in a different repository, contributors need to first create an issue in the same repository and use this issue to track.

If the pull request body does not provide the required content, the bot will add the `do-not-merge/needs-linked-issue` label to the pull request to prevent it from being merged.

### Format of the commit message

The bot we use will extract the pull request title as the one-line subject and messages inside the `commit-message` code block as commit message body. For example, a pull request with title `pkg: what's changed in this one package` and a body containing:

    ```commit-message
    any multiple line commit messages that go into
    the final commit message body

    * fix something 1
    * fix something 2
    ```

will get a final commit message:

```
pkg: what's changed in this one package (#12345)

any multiple line commit messages that go into
the final commit message body

* fix something 1
* fix something 2
```

The first line is the subject (the pull request title) and should be no longer than 70 characters, the second line is always blank, and other lines should be wrapped at 80 characters. This allows the message to be easier to read on GitHub as well as in various git tools.

If the change affects more than one subsystem, you can use a comma to separate them like `server, pd-client:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

The body of the commit message should describe why the change was made and at a high level, how the code works.

### Signing off the commit

The project uses [DCO check](https://github.com/probot/dco#how-it-works) and the commit message must contain a `Signed-off-by` line for [Developer Certificate of Origin](https://developercertificate.org/).

Use option `git commit -s` to sign off your commits.
