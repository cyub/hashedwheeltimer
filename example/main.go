package main

import (
	"fmt"
	"time"

	"github.com/cyub/hashedwheeltimer"
)

type job func(timeout hashedwheeltimer.Timeout) bool

func (j job) Run(timeout hashedwheeltimer.Timeout) bool {
	return j(timeout)
}

func main() {
	hwt := hashedwheeltimer.NewHashedWheelTimer(time.Second, 16)
	fn1 := func(timeout hashedwheeltimer.Timeout) bool {
		fmt.Println(time.Now(), "job1 run")
		timeout.Timer().NewTimeout(timeout.Task(), time.Duration(timeout.Deadline()))
		return true
	}

	fn2 := func(timeout hashedwheeltimer.Timeout) bool {
		fmt.Println(time.Now(), "job2 run")
		return true
	}

	hwt.NewTimeout(job(fn1), time.Second)
	hwt.NewTimeout(job(fn2), 3*time.Second)
	time.Sleep(15 * time.Second)
}
