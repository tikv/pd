This is a fuzz test for new GC API

It calls SetGCBarrier/AdvanceTxnSafePoint/AdvanceGCSafePoint/GetGCState etc, and check some invariances.
At the same time, it run pd resign leader.
