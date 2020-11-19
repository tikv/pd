<!--
Thank you for working on PD! Please read PD's [CONTRIBUTING](https://github.com/tikv/pd/blob/master/CONTRIBUTING.md) document **BEFORE** filing this PR.
PR Title Format:
1. pkg [, pkg2, pkg3]: what's changed
2. *: what's changed


If you want to open the **Challenge Program** pull request, please use the following template:
https://raw.githubusercontent.com/tikv/.github/master/.github/PULL_REQUEST_TEMPLATE/challenge-program.md
You can use it with query parameters: https://github.com/tikv/pd/compare/master...${you branch}?template=challenge-program.md
-->

### What problem does this PR solve?

<!-- Add the issue link with a summary if it exists. -->

### What is changed and how it works?

### Check List

<!-- Remove the items that are not applicable. -->

Tests

<!-- At least one of them must be included. -->

- Unit test
- Integration test
- Manual test (add detailed scripts or steps below)
- No code

Code changes

- Has configuration change
- Has HTTP API interfaces change (Don't forget to [add the declarative for API](https://github.com/tikv/pd/blob/master/docs/development.md#updating-api-documentation))
- Has persistent data change

Side effects

- Possible performance regression
- Increased code complexity
- Breaking backward compatibility

Related changes

- PR to update [`pingcap/docs`](https://github.com/pingcap/docs)/[`pingcap/docs-cn`](https://github.com/pingcap/docs-cn):
- PR to update [`pingcap/tidb-ansible`](https://github.com/pingcap/tidb-ansible):
- Need to cherry-pick to the release branch

### Release note

<!-- A bugfix or a new feature needs a release note. If there is no need release note, just uncomment the below line. -->

<!-- - No release note -->
