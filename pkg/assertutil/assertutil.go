package assertutil

// Checker accepts the injection of check functions and context from test files
type Checker struct {
	IsNil     func(obtained interface{})
	Equal     func(obtained, expected interface{})
	DeepEqual func(obtained, expected interface{})
	FailNow   func()
}

func NewChecker(failnow func()) *Checker {
	return &Checker{
		FailNow: failnow,
	}
}

func (c *Checker) failNow() {
	c.FailNow()
}

func (c *Checker) AssertNil(obtained interface{}) {
	if c.IsNil == nil {
		c.failNow()
	}
	c.IsNil(obtained)
}

func (c *Checker) AssertEqual(obtained, expected interface{}) {
	if c.Equal == nil {
		c.failNow()
	}
	c.Equal(obtained, expected)
}

func (c *Checker) AssertDeepEqual(obtained, expected interface{}) {
	if c.DeepEqual == nil {
		c.failNow()
	}
	c.DeepEqual(obtained, expected)
}
