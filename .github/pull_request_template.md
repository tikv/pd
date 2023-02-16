<!--

Thank you for working on PD! Please read PD's [CONTRIBUTING](https://github.com/tikv/pd/blob/master/CONTRIBUTING.md) document **BEFORE** filing this PR.

PR Title Format:
1. pkg [, pkg2, pkg3]: what's changed
2. *: what's changed

-->

### What problem does this PR solve?

<!--

Please create an issue first to describe the problem.
There MUST be one line starting with "Issue Number:  " and 
linking the relevant issues via the "close" or "ref".
For more info, check https://github.com/tikv/pd/blob/master/CONTRIBUTING.md#linking-issues.

-->
Issue Number: Close #xxx

### What is changed and how does it work?

<!--

You could use the "commit message" code block to add more description to the final commit message.
For more info, check https://github.com/tikv/pd/blob/master/CONTRIBUTING.md#format-of-the-commit-message.

-->

```commit-message
```

### Check List

<!-- Remove the items that are not applicable. -->

Tests

<!-- At least one of these tests must be included. -->

- Unit test
- Integration test
- Manual test (add detailed scripts or steps below)
- No code

Code changes

- Has the configuration change
- Has HTTP API interfaces changed (Don't forget to [add the declarative for the new API](https://github.com/tikv/pd/blob/master/docs/development.md#updating-api-documentation))
- Has persistent data change

Side effects

- Possible performance regression
- Increased code complexity
- Breaking backward compatibility

Related changes

- PR to update [`pingcap/docs`](https://github.com/pingcap/docs)/[`pingcap/docs-cn`](https://github.com/pingcap/docs-cn):
- PR to update [`pingcap/tiup`](https://github.com/pingcap/tiup):
- Need to cherry-pick to the release branch

### Release note

<!--

A bugfix or a new feature needs a release note. If there is no need to give a release note, just leave it with the `None`.

Please refer to [Release Notes Language Style Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html) to write a quality release note.

-->

```release-note
None.
```
