package server

// var _ = Suite(&testHotRegionGrpcServer{})

// type testHotRegionGrpcServer struct {
// 	clean   func()
// 	storage *cluster.HotRegionStorage
// 	sever   *grpc.Server
// 	address string
// }

// func (h *testHotRegionGrpcServer) SetUpSuite(c *C) {
// 	storage, clean := newTestHotRegionStorage(c)
// 	h.clean = clean
// 	h.storage = storage
// 	server := grpc.NewServer()
// 	hotregionhistory.RegisterHotRegionsHistoryServer(server, &hotRegionHistoryServer{
// 		storage: storage,
// 	})
// 	listener, err := net.Listen("tcp", ":0")
// 	c.Assert(err, IsNil, Commentf("cannot find available port"))
// 	h.sever = server
// 	h.address = fmt.Sprintf(":%d", listener.Addr().(*net.TCPAddr).Port)
// 	go func() {
// 		if err := server.Serve(listener); err != nil {
// 			log.Fatalf("failed to serve: %v", err)
// 		}
// 	}()
// }

// func (h *testHotRegionGrpcServer) TestHotRegionGrpcServer(c *C) {
// 	// db := h.storage.LeveldbKV
// 	// batch := new(leveldb.Batch)
// }
// func newTestHotRegionStorage(c *C) (*cluster.HotRegionStorage, func()) {
// 	ctx := context.Background()
// 	storage, err := cluster.NewHotRegionsHistoryStorage(ctx,
// 		"./tmp", nil, nil, nil, 30, time.Hour)
// 	c.Assert(err, IsNil)
// 	clean := func() {
// 		os.RemoveAll("./tmp")
// 		ctx.Done()
// 	}
// 	return storage, clean
// }
