package command

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&EtcdCtl{})

type EtcdCtl struct{}

func (s *EtcdCtl) TestBase64Encode(c *C) {
	c.Parallel()
	expected := "SGVsbG8sV29ybGQ="
	obtained := base64Encode("Hello,World")
	c.Assert(obtained, Equals, expected)
}

func (s *EtcdCtl) TestBase64Decode(c *C) {
	c.Parallel()
	expected := "Hello,World"
	obtained, err := base64Decode("SGVsbG8sV29ybGQ=")
	c.Assert(err, IsNil)
	c.Assert(obtained, Equals, expected)
	obtained, err = base64Decode("ThisIsNotBase64")
	c.Assert(obtained, Equals, "")
	c.Assert(err, ErrorMatches, "*illegal.*")

}
