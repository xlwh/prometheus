package main

import (
	"fmt"
	"time"
)

var (
	funcCh chan func()
)

func main() {
	funcCh = make(chan func(), 100)
	go run()
	for i := 0; i < 100; i++ {
		funcCh <- say
		funcCh <- hi
		time.Sleep(time.Second * 2)
	}

}

func say() {
	fmt.Println("hello")
}

func hi() {
	fmt.Println("world")
}

func run() {
	for {
		select {
		case f := <-funcCh:
			f()
		}
	}
}
