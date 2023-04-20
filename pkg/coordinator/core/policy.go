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

package core

import (
	"k8s.io/apimachinery/pkg/util/sets"
)

/* In this file, we implement several queue selection algorithms. */

type queueSelector interface {
	Next(queues map[string]*queue) (string, *queue)
}

/* The RoundRobin policy. */

func newRoundRobinSelector() queueSelector {
	return &roundRobinSelector{lastSelected: -1}
}

var _ queueSelector = &roundRobinSelector{}

type roundRobinSelector struct {
	queueNames   []string
	lastSelected int
}

func (rr *roundRobinSelector) Next(queues map[string]*queue) (string, *queue) {
	if rr.queuesChanged(queues) {
		rr.appendNewQueues(queues)
	}

	if len(rr.queueNames) == 0 {
		return "", nil
	}

	idx := (rr.lastSelected + 1) % len(rr.queueNames)
	name := rr.queueNames[idx]
	q := queues[name]
	rr.lastSelected++

	return name, q
}

func (rr *roundRobinSelector) queuesChanged(queues map[string]*queue) bool {
	if len(rr.queueNames) == 0 {
		return true
	}
	// Queue will not be released once created, hence length of queues changes indicates that
	// we should update queueNames slice.
	return len(rr.queueNames) != len(queues)
}

// appendNewQueues append new incoming queues and provides stable order for round-robin selection.
func (rr *roundRobinSelector) appendNewQueues(queues map[string]*queue) {
	existQueues := sets.NewString(rr.queueNames...)
	for curQueueName := range queues {
		if !existQueues.Has(curQueueName) {
			rr.queueNames = append(rr.queueNames, curQueueName)
		}
	}
}

/* The WightedRoundRobin policy. */

func newWeightedRoundRobinSelector() queueSelector {
	return &weightedRoundRobinSelector{
		curSelected: -1,
		curWeight:   0,
	}
}

var _ queueSelector = &weightedRoundRobinSelector{}

type weightedRoundRobinSelector struct {
	queueNames       []string
	queueWeights     map[string]int
	weightsSlice     []int
	queueNameToIndex map[string]int

	curSelected int
	curWeight   int

	// used for select next queue
	weightGcd int
	weightMax int
	weightSum int
}

func (wrr *weightedRoundRobinSelector) Next(queues map[string]*queue) (string, *queue) {
	if wrr.queuesChanged(queues) {
		wrr.appendNewQueuesOrUpdate(queues)
	}

	if len(wrr.queueNames) == 0 {
		return "", nil
	} else {
		// update (TODO: the performance here can be improved by eliminating redundant calculation)
		wrr.weightGcd = getGcd(wrr.weightsSlice)
		wrr.weightMax = getMaxWeight(wrr.weightsSlice)
		wrr.weightSum = getWeightSum(wrr.weightsSlice)
	}

	idx := wrr.nextQueueIndex()
	name := wrr.queueNames[idx]
	q := queues[name]

	return name, q
}

func (wrr *weightedRoundRobinSelector) queuesChanged(queues map[string]*queue) bool {
	if len(wrr.queueNames) == 0 || len(wrr.queueNames) != len(queues) {
		return true
	}
	// Even though no new queue is created, existing queues can be changed.
	// For example, more QueueUnits are added into. In this case, we
	// need to update the weights.
	changed := false
	for qName, q := range queues {
		if wrr.queueWeights[qName] != calculateQueueWeight(q) {
			changed = true
			break
		}
	}
	return changed
}

// appendNewQueuesOrUpdate appends new incoming queues or updates existing queues' weights,
// and provides stable order for weighted round-robin selection.
func (wrr *weightedRoundRobinSelector) appendNewQueuesOrUpdate(queues map[string]*queue) {
	existQueues := sets.NewString(wrr.queueNames...)
	for qName, q := range queues {
		if !existQueues.Has(qName) {
			// This is a new queue, just add it
			wrr.queueNames = append(wrr.queueNames, qName)
			// calculate the weight of current queue with its total pending resource requests.
			// TODO: Find a property way to add difference kinds of resources together.
			//  Currently, we just use total number of tasks to represent its weight.
			//  Besides, the task status can be changed. We should not count the non-pending tasks in.
			//  Take a look at the expire() function of assumedQuota.
			w := calculateQueueWeight(q)
			wrr.queueWeights[qName] = w
			wrr.weightsSlice = append(wrr.weightsSlice, w)
			wrr.queueNameToIndex[qName] = len(wrr.queueNames) - 1
		} else {
			// This is an existing queue, update its weight if necessary
			w := calculateQueueWeight(q)
			if wrr.queueWeights[qName] != w {
				wrr.queueWeights[qName] = w
				wrr.weightsSlice[wrr.queueNameToIndex[qName]] = w
			}
		}
	}
}

func gcd(a, b int) int {
	if b == 0 {
		return a
	}
	return gcd(b, a%b)
}

func getGcd(weights []int) int {
	ret := weights[0]
	for idx := 1; idx < len(weights); idx++ {
		ret = gcd(ret, weights[idx])
	}
	return ret
}

func getMaxWeight(weights []int) int {
	ret := -1
	for _, w := range weights {
		if w > ret {
			ret = w
		}
	}
	return ret
}

func getWeightSum(weights []int) int {
	ret := 0
	for _, w := range weights {
		ret += w
	}
	return ret
}

func (wrr *weightedRoundRobinSelector) nextQueueIndex() int {
	for true {
		wrr.curSelected = (wrr.curSelected + 1) % len(wrr.weightsSlice)
		if wrr.curSelected == 0 {
			wrr.curWeight -= wrr.weightGcd
			if wrr.curWeight <= 0 {
				wrr.curWeight = wrr.weightMax
				if wrr.curWeight == 0 {
					return -1
				}
			}
		}
		if wrr.weightsSlice[wrr.curSelected] >= wrr.curWeight {
			return wrr.curSelected
		}
	}
	// TODO: Check whether this line of code can never be reached
	return wrr.curSelected
}

// calculateQueueWeight calculates the weight of a given queue.
func calculateQueueWeight(q *queue) int {
	totalPendingTasksNum := 0
	for _, qu := range q.units {
		totalPendingTasksNum += len(qu.Tasks)
	}
	return totalPendingTasksNum
}

// TODO: Implement SmoothWeightedRoundRobin policy.
