// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package realcluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type tsSuite struct {
	clusterSuite
}

func TestTS(t *testing.T) {
	suite.Run(t, &tsSuite{
		clusterSuite: clusterSuite{
			suiteName: "ts",
		},
	})
}

func (s *tsSuite) TestTS() {
	re := require.New(s.T())

	db := OpenTestDB(s.T())
	db.MustExec("use test")
	db.MustExec("drop table if exists t")
	db.MustExec("create table t(a int, index i(a))")
	db.MustExec("insert t values (1), (2), (3)")
	var rows int
	err := db.inner.Raw("select count(*) from t").Row().Scan(&rows)
	re.NoError(err)
	re.Equal(3, rows)

	re.NoError(err)
	re.Equal(3, rows)

	var ts uint64
	err = db.inner.Begin().Raw("select @@tidb_current_ts").Scan(&ts).Rollback().Error
	re.NoError(err)
	re.NotEqual(0, GetTimeFromTS(ts))

	db.MustClose()
}

func TestMSTS(t *testing.T) {
	suite.Run(t, &tsSuite{
		clusterSuite: clusterSuite{
			suiteName: "ts",
			ms:        true,
		},
	})
}
