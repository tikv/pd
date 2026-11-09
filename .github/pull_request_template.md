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
- Has HTTP APIs changed (Don't forget to [add the declarative for the new API](https://github.com/tikv/pd/blob/master/docs/development.md#updating-api-documentation))
- Has persistent data change

Side effects

- Possible performance regression
- Increased code complexity
- Breaking backward compatibility

Related changes

- PR to update [`pingcap/docs`](https://github.com/pingcap/docs)/[`pingcap/docs-cn`](https://github.com/pingcap/docs-cn):
- PR to update [`pingcap/tiup`](https://github.com/pingcap/tiup):
- Need to cherry-pick to the release branch

### Design trade-offs

<!--

Alternatives a reviewer is likely to ask about, and why they do not work.
The reason for the final approach goes in the commit message above. Write `None` if there is no real alternative.

-->

### Verification

<!--

What the tests show, beyond the test types listed above. One row per claim, meaning something this PR must make true,
with the test or command that shows it, its result,
and any condition it needs (for example, failpoints enabled or `NEXT_GEN=1`). Put manual test steps here.
For a bug fix, show that the new test fails without the fix, or explain why it cannot.
Commit is the commit the evidence was collected on; append `(on <commit>)` to a row that used a different one.

-->

Commit:

| Claim | Evidence |
| --- | --- |
|  |  |

Not verified:

### Risks and rollback

<!--

Explain the items kept under Code changes and Side effects, plus any other failure mode.
Say whether the change can be rolled back, and whether downgrading PD is still safe.
Write `None` if there is nothing beyond the check list.

-->

### Questions for reviewers

<!--

Decisions a reviewer needs to make, such as a changed contract, a compatibility trade-off, or a residual risk to accept,
and the places the author is least sure about. This is where to start reviewing, not a limit on what to review.
Write `None` if there is nothing to decide.

-->

### Release note

<!--

A bugfix or a new feature needs a release note. If there is no need to give a release note, just leave it with the `None`.

Please refer to [Release Notes Language Style Guide](https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html) to write a quality release note.

-->

```release-note
None.
```
