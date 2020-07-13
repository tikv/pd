package server

import (
	"github.com/joomcode/errorx"
)

var (
	serverErrors = errorx.NewNamespace("server")

	// ErrRegionNotFound is error info for region not found.
	ErrRegionNotFound = serverErrors.NewType("region_not_found")
	// ErrStoreNotFound is error info for store not found.
	ErrStoreNotFound = serverErrors.NewType("store_not_found")
	// ErrPluginNotFound is error info for plugin not found.
	ErrPluginNotFound = serverErrors.NewType("plugin_not_found")
	// ErrServerNotStarted is error info for server not started.
	ErrServerNotStarted = serverErrors.NewType("server_not_started")
	// ErrOperatorNotFound is error info for operator not found.
	ErrOperatorNotFound = serverErrors.NewType("operator_not_found")
	// ErrAddOperator is error info for already have an operator when adding operator.
	ErrAddOperator = serverErrors.NewType("operator_existed")
	// ErrRegionNotAdjacent is error info for region not adjacent.
	ErrRegionNotAdjacent = serverErrors.NewType("regions_not_adjacent")
	// ErrRegionAbnormalPeer is error info for region has abonormal peer.
	ErrRegionAbnormalPeer = serverErrors.NewType("region_has_abnormal_peer")

	// NewStoreNotFoundErr is a wrap function for ErrStoreNotFound.
	NewStoreNotFoundErr = func(storeID uint64) error {
		return ErrStoreNotFound.New("store id: %d", storeID)
	}
	// NewRegionNotFoundErr is a wrap function for ErrRegionNotFound.
	NewRegionNotFoundErr = func(regionID uint64) error {
		return ErrRegionNotFound.New("region id: %d", regionID)
	}
	// NewOperatorNotFoundErr is a wrap function for ErrOperatorNotFound.
	NewOperatorNotFoundErr = func(regionID uint64) error {
		return ErrOperatorNotFound.New("region id: %d", regionID)
	}
	// NewPluginNotFoundErr is a wrap function for ErrPluginNotFound.
	NewPluginNotFoundErr = func(pluginPath string) error {
		return ErrPluginNotFound.New("plugin path: %s", pluginPath)
	}
	// NewRegionAbnormalPeerErr is a wrap function for ErrRegionAbnormalPeer.
	NewRegionAbnormalPeerErr = func(regionID uint64) error {
		return ErrRegionAbnormalPeer.New("region id: %d", regionID)
	}
	// NewAddOperatorErr is a wrap function for ErrAddOperator.
	NewAddOperatorErr = ErrAddOperator.NewWithNoMessage()
	// NewRegionNotAdjacentErr is a wrap function for ErrRegionNotAdjacent.
	NewRegionNotAdjacentErr = ErrRegionNotAdjacent.NewWithNoMessage()
	// NewServerNotStartedErr is a wrap function for ErrServerNotStarted.
	NewServerNotStartedErr = ErrServerNotStarted.NewWithNoMessage()
)
