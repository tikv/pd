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

package retry

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Config is the configuration of the Backoff function.
type Config struct {
	name  string
	fnCfg *BackoffFnCfg
	err   error
}

// Backoff Config variables
var (
	// BoMemberUpdate is for member change events.
	BoMemberUpdate = NewConfig("memberUpdate", NewBackoffFnCfg(100, 2000, EqualJitter), nil)
)

// backoffFn is the backoff function which compute the sleep time and do sleep.
type backoffFn func(ctx context.Context, maxSleepMs int) int

func (c *Config) createBackoffFn() backoffFn {
	return newBackoffFn(c.fnCfg.base, c.fnCfg.cap, c.fnCfg.jitter)
}

// BackoffFnCfg is the configuration for the backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
type BackoffFnCfg struct {
	base   int
	cap    int
	jitter int
}

// NewBackoffFnCfg creates the config for BackoffFn.
func NewBackoffFnCfg(base, cap, jitter int) *BackoffFnCfg {
	return &BackoffFnCfg{
		base,
		cap,
		jitter,
	}
}

// NewConfig creates a new Config for the Backoff operation.
func NewConfig(name string, backoffFnCfg *BackoffFnCfg, err error) *Config {
	return &Config{
		name:  name,
		fnCfg: backoffFnCfg,
		err:   err,
	}
}

func (c *Config) String() string {
	return c.name
}

// SetErrors sets a more detailed error instead of the default bo config.
func (c *Config) SetErrors(err error) {
	c.err = err
}

const (
	// NoJitter makes the backoff sequence strict exponential.
	NoJitter = 1 + iota
	// FullJitter applies random factors to strict exponential.
	FullJitter
	// EqualJitter is also randomized, but prevents very short sleeps.
	EqualJitter
	// DecorrJitter increases the maximum jitter based on the last random value.
	DecorrJitter
)

// newBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func newBackoffFn(base, cap, jitter int) backoffFn {
	if base < 2 {
		// Top prevent panic in 'rand.Intn'.
		base = 2
	}
	attempts := 0
	lastSleep := base
	return func(ctx context.Context, maxSleepMs int) int {
		var sleep int
		switch jitter {
		case NoJitter:
			sleep = expo(base, cap, attempts)
		case FullJitter:
			v := expo(base, cap, attempts)
			sleep = rand.Intn(v)
		case EqualJitter:
			v := expo(base, cap, attempts)
			sleep = v/2 + rand.Intn(v/2)
		case DecorrJitter:
			sleep = int(math.Min(float64(cap), float64(base+rand.Intn(lastSleep*3-base))))
		}
		log.Debug("backoff",
			zap.Int("base", base),
			zap.Int("sleep", sleep),
			zap.Int("attempts", attempts))

		realSleep := sleep
		// when set maxSleepMs >= 0 will force sleep maxSleepMs milliseconds.
		if maxSleepMs >= 0 && realSleep > maxSleepMs {
			realSleep = maxSleepMs
		}
		select {
		case <-time.After(time.Duration(realSleep) * time.Millisecond):
			attempts++
			lastSleep = sleep
			return realSleep
		case <-ctx.Done():
			return 0
		}
	}
}

func expo(base, cap, n int) int {
	return int(math.Min(float64(cap), float64(base)*math.Pow(2.0, float64(n))))
}
