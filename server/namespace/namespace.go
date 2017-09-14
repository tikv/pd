package namespace

import "github.com/pingcap/pd/server/core"

// Namespace defines two things:
// 1. relation between a name and several tables
// 2. relation between a name and several stores
// It is used to bind tables with stores
type Namespace struct {
	Name string
	TableIDs []uint16
	StoreIDs []uint64
}

type TableNameSpaces []Namespace
var tableNamespaces TableNameSpaces

// Implement Classifier interface
func (namespaces TableNameSpaces) GetStoreNamespace(info *core.StoreInfo) string {
	// Find a namespace that contains the store id.
	// Do we need a lock here?
	return nil
}

func (namespaces TableNameSpaces) GetRegionNamespace(*core.RegionInfo) string {
	// Find a namespace that contains the region's start_key and end_key
	// Do we need a lock here?
	return nil
}

// List all table namespace

// Create a table Namespace and append it into tableNamespaces
// Store it in etcd.

// Add tableID to the namespace, we need to validate that tableID is unique
// across all namespaces. Do we need a lock here?
// Store it in etcd.

// Add storeID to the namespace, we need to validate that storeID is unique
// across all namespaces. Do we need a lock here?
// Store it in etcd.

// Remove tableID from the namespace

// Remove storeID from the namespace

// Move tableID from one namespace to another namespace

// Move storeID from one namespace to another namespace


// Load namespace from etcd here?
func GetClassifier() Classifier {
	if tableNamespaces == nil {
		return DefaultClassifier
	}
	return tableNamespaces
}
