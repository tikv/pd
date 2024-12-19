// Copyright 2023 TiKV Authors
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
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"moul.io/zapgorm2"

	"github.com/pingcap/log"
)

// TestDB is a test database
type TestDB struct {
	inner   *gorm.DB
	require *require.Assertions

	isUnderlyingMocked bool
	mock               sqlmock.Sqlmock
}

// OpenTestDB opens a test database
func OpenTestDB(t *testing.T, configModifier ...func(*mysqldriver.Config, *gorm.Config)) *TestDB {
	r := require.New(t)

	dsn := mysqldriver.NewConfig()
	dsn.Net = "tcp"
	dsn.Addr = "127.0.0.1:4000"
	dsn.Params = map[string]string{"time_zone": "'+00:00'"}
	dsn.ParseTime = true
	dsn.Loc = time.UTC
	dsn.User = "root"
	dsn.DBName = "test"

	config := &gorm.Config{
		Logger: zapgorm2.New(log.L()),
	}

	for _, m := range configModifier {
		m(dsn, config)
	}

	db, err := gorm.Open(mysql.Open(dsn.FormatDSN()), config)
	r.NoError(err)

	return &TestDB{
		inner:   db.Debug(),
		require: r,
	}
}

// MustClose closes the test database
func (db *TestDB) MustClose() {
	if db.isUnderlyingMocked {
		db.mock.ExpectClose()
	}

	d, err := db.inner.DB()
	db.require.NoError(err)

	err = d.Close()
	db.require.NoError(err)
}

// Gorm returns the underlying gorm.DB
func (db *TestDB) Gorm() *gorm.DB {
	return db.inner
}

// MustExec executes a query
func (db *TestDB) MustExec(sql string, values ...any) {
	err := db.inner.Exec(sql, values...).Error
	db.require.NoError(err)
}
