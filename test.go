package main

import (
	"fmt"
	"time"
)

func main() {
	MaxQueueSize := 10000
	c := make(chan time.Time, MaxQueueSize) //max_size
	finish := make(chan struct{})

	count := 1000
	go func() {
		lans := 0.0
		for i := 0; i < count; i += 1 {
			// now := time.Now()
			now := <-c
			if now.After(time.Now()) {
				continue
			}
			time.Sleep(10 * time.Millisecond)
			lans += time.Since(now).Seconds()
		}
		fmt.Println("recv: avg lat", lans/float64(count)*1000.0, "ms")
		finish <- struct{}{}
	}()

	lans := 0.0
	now := time.Now()
	for i := 0; i < count; i += 1 {
		c <- now
		now = now.Add(10 * time.Millisecond)
		lans += time.Since(now).Seconds()
	}
	fmt.Println("send: avg lat", lans/float64(count)*1000.0, "ms")

	<-finish
}
