package server

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type tableNamespaceClassifier struct {
	nsInfo *namespacesInfo
}

func newTableNamespaceClassifier(nsInfo *namespacesInfo) tableNamespaceClassifier {
	return tableNamespaceClassifier{
		nsInfo,
	}
}

func (classifier tableNamespaceClassifier) GetAllNamespaces() []string {
	nsList := make([]string, len(classifier.nsInfo.namespaces))
	for name := range classifier.nsInfo.namespaces {
		nsList = append(nsList, name)
	}
	return nsList
}

func (classifier tableNamespaceClassifier) GetStoreNamespace(storeInfo *core.StoreInfo) string {
	for name, ns := range classifier.nsInfo.namespaces {
		if storeInfo.Id == ns.ID {
			return name
		}
	}
	return namespace.DefaultNamespace
}

func (classifier tableNamespaceClassifier) GetRegionNamespace(regionInfo *core.RegionInfo) string {
	for name, ns := range classifier.nsInfo.namespaces {
		startTable := core.DecodeTableID(regionInfo.StartKey)
		endTable := core.DecodeTableID(regionInfo.EndKey)
		for _, tableID := range ns.TableIDs {
			if tableID == startTable && tableID == endTable {
				return name
			}
		}
	}
	return namespace.DefaultNamespace
}
