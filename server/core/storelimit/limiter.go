package storelimit

type Limiter interface {
	Available(n int64) bool
	Take(count int64) bool
}
