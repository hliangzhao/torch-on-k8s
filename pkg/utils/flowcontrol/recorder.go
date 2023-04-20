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

package flowcontrol

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"sync"
	"time"
)

/* The flow control event recorder. */

func NewFlowControlRecorder(recorder record.EventRecorder, qps int32) record.EventRecorder {
	if qps <= 0 {
		qps = 10
	}
	return &flowControlRecorder{
		qps:            qps,
		recorder:       recorder,
		inflightEvents: map[types.UID]event{},
		notify:         *sync.NewCond(&sync.Mutex{}),
	}
}

var _ record.EventRecorder = &flowControlRecorder{}

type flowControlRecorder struct {
	qps            int32
	recorder       record.EventRecorder
	inflightEvents map[types.UID]event
	notify         sync.Cond
	once           sync.Once
}

// Event emits a pending event to record and save the given event into the recorder (wait for next emit).
func (f *flowControlRecorder) Event(object runtime.Object, eventType, reason, message string) {
	// only one event will be emitted and recorded each time
	f.once.Do(func() {
		go wait.UntilWithContext(context.Background(), f.emitOne, time.Second/time.Duration(f.qps))
	})

	metaObj, ok := object.(metav1.Object)
	if !ok {
		f.recorder.Event(object, eventType, reason, message)
		return
	}

	f.notify.L.Lock()
	defer f.notify.L.Unlock()

	// toNotify implies that previous pending events had all cleared out, and
	// emitOne() routine waits for being notified to consume events.
	toNotify := len(f.inflightEvents) == 0

	// save the give event into recorder
	f.inflightEvents[metaObj.GetUID()] = event{
		obj:       object,
		eventType: eventType,
		reason:    reason,
		msg:       message,
	}

	if toNotify {
		f.notify.Signal()
	}
}

// emitOne randomly chooses one pending event and emit it through real event recorder.
func (f *flowControlRecorder) emitOne(_ context.Context) {
	if len(f.inflightEvents) == 0 {
		f.notify.Wait()
	}

	for uid, event := range f.inflightEvents {
		f.recorder.Event(event.obj, event.eventType, event.reason, event.msg)
		f.notify.L.Lock()
		delete(f.inflightEvents, uid)
		if len(f.inflightEvents) == 0 {
			f.notify.Wait()
		}
		f.notify.L.Unlock()
		return
	}
}

func (f *flowControlRecorder) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...interface{}) {
	f.Event(object, eventType, reason, fmt.Sprintf(messageFmt, args...))
}

func (f *flowControlRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string,
	eventType, reason, messageFmt string, args ...interface{}) {

	f.recorder.AnnotatedEventf(object, annotations, eventType, reason, messageFmt, args...)
}

// event defines the event struct.
type event struct {
	obj       runtime.Object
	eventType string
	reason    string
	msg       string
}
