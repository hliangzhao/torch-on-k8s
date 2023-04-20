/*
Copyright 2023 Hailiang Zhao.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package concurrent

import "sync"

func NewSemaphore(tickets int) *semaphore {
	return &semaphore{
		tickets: make(chan struct{}, tickets),
	}
}

type semaphore struct {
	tickets chan struct{}
	wg      sync.WaitGroup
}

func (sema *semaphore) Acquire() {
	sema.tickets <- struct{}{}
	sema.wg.Add(1)
}

func (sema *semaphore) Release() {
	<-sema.tickets
	sema.wg.Done()
}

func (sema *semaphore) Wait() {
	sema.wg.Wait()
	close(sema.tickets)
}
