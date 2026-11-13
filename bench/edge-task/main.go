package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
)

func burnCPU(d time.Duration, threads int) {
	deadline := time.Now().Add(d)
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := 1.0
			for time.Now().Before(deadline) {
				for j := 0; j < 10000; j++ {
					x = math.Sqrt(x*x + 1)
				}
			}
			_ = x
		}()
	}
	wg.Wait()
}

func allocMemory(mib int) []byte {
	buf := make([]byte, mib*1024*1024)
	for i := 0; i < len(buf); i += 4096 {
		buf[i] = 1
	}
	return buf
}

func main() {
	cpu := flag.Duration("cpu", 0, "how long to keep the CPU busy")
	threads := flag.Int("threads", 1, "number of busy threads")
	mem := flag.Int("mem", 0, "memory to allocate in MiB")
	sleep := flag.Duration("sleep", 0, "how long to sleep after the CPU work")
	exitCode := flag.Int("exit", 0, "exit code")
	out := flag.String("out", "", "text printed at the end")
	flag.Parse()

	start := time.Now()
	fmt.Printf("edge-task start: cpu=%s threads=%d mem=%dMiB sleep=%s\n", *cpu, *threads, *mem, *sleep)

	buf := allocMemory(*mem)
	burnCPU(*cpu, *threads)
	time.Sleep(*sleep)
	runtime.KeepAlive(buf)

	if *out != "" {
		fmt.Println(*out)
	}
	fmt.Printf("edge-task done in %s\n", time.Since(start).Round(time.Millisecond))
	os.Exit(*exitCode)
}
