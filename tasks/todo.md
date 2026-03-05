# Fix Flaky Tests - Task Tracker

## Completed Fixes
- [x] #5304 StoreStateFilterReasonTest - Fixed Source→Target bug in check function
- [x] #8512 TestResourceGroupController - Fixed re.EqualError inside Eventually, added nil checks
- [x] #9800 TestMultipleServerTestSuite - Fixed deadlock by reducing lock scope in Run()
- [x] #9316 TestMergeKeyspaceGroup - Replaced re.NoError inside Eventually with plain Go checks
- [x] #8792 TestResourceManagerClientTestSuite panic - Added re.Error() before err.Error() call
- [x] #9583 Go leak in resource_group/controller - Tracked goroutine with c.wg
- [x] #10244 TestGetAllKeyspaceGCStates - Dropped from this PR (fixed in #10244)
- [x] #10247 TestConfigTTLAfterTransferLeader - Dropped from this PR (fixed in #10274)
- [x] #8035 TestConfigController - Wrapped checkShow with testutil.Eventually
- [x] #8739 TestResourceGroupRUConsumption - Replaced hard sleep with Eventually
- [x] #9584 Panic in pd-ctl scheduler - Added safe type assertions
- [x] #8103 TestRandomShutdown - Replaced re.Less/re.NotEmpty in goroutines with plain Go
- [x] #9533 TestMixedTSODeployment - Same fix as #8103 (uses same checkTSO function)

## Already Fixed / Skipped
- [x] #8389 TestReloadLabel - Already fixed in master
- [x] #6620 TestSwitchBurst - Already skipped with suite.T().Skip()
- [x] #9591 TestAPI/TestStores - Excluded by user (already fixed)

## Deferred (too complex for this PR)
- [ ] #9645 TestTIKVEngine - Randomization in scheduler, needs deeper fix
- [ ] #8967 TestForwardTSOUnexpectedToFollower1 - Complex gRPC cluster ID issue
- [ ] #5028 Data race in CI - Systemic issue, too broad
- [ ] #9443 TestRegionSyncer - Complex goroutine lifecycle management
- [ ] #9960 Data race in TestKeyspaceServiceLimitAPI - Complex concurrency model

## Phase 3: Submit
- [ ] make check passes
- [ ] Push and create PR
