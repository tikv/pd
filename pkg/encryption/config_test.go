// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encryption

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/typeutil"
)

func TestConfig(t *testing.T) {
	TestingT(t)
}

type testConfigSuite struct{}

var _ = Suite(&testConfigSuite{})

func (s *testConfigSuite) TestAdjustDefaultValue(c *C) {
	config := &Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	c.Assert(config.DataEncryptionMethod, Equals, methodPlaintext)
	defaultRotationPeriod, _ := time.ParseDuration(defaultDataKeyRotationPeriod)
	c.Assert(config.DataKeyRotationPeriod.Duration, Equals, defaultRotationPeriod)
	c.Assert(config.MasterKey.Type, Equals, masterKeyTypePlaintext)
}

func (s *testConfigSuite) TestAdjustInvalidDataEncryptionMethod(c *C) {
	config := &Config{DataEncryptionMethod: "unknown"}
	c.Assert(config.Adjust(), NotNil)
}

func (s *testConfigSuite) TestAdjustNegativeRotationDuration(c *C) {
	config := &Config{DataKeyRotationPeriod: typeutil.NewDuration(time.Duration(int64(-1)))}
	c.Assert(config.Adjust(), NotNil)
}

func (s *testConfigSuite) TestAdjustInvalidMasterKeyType(c *C) {
	config := &Config{MasterKey: MasterKeyConfig{Type: "unknown"}}
	c.Assert(config.Adjust(), NotNil)
}
