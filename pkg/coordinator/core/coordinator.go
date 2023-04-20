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
	"context"
	"fmt"
	commonapis "github.com/hliangzhao/torch-on-k8s/pkg/common/apis/v1alpha1"
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"github.com/hliangzhao/torch-on-k8s/pkg/coordinator/plugins"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sync"
)

/* Coordinator implementation. */

var (
	globalCoordinator pkgcoordinator.Coordinator
	log               = logf.Log.WithName("job-coordinator")
)

var _ pkgcoordinator.Coordinator = &Coordinator{}

// NewCoordinator initializes the globalCoordinator and starts running it as a go routine.
func NewCoordinator(mgr manager.Manager) pkgcoordinator.Coordinator {
	if globalCoordinator != nil {
		return globalCoordinator
	}

	c := mgr.GetClient()
	recorder := mgr.GetEventRecorderFor("JobCoordinator")
	co := &Coordinator{
		config:           plugins.NewCoordinateConfiguration(),
		queues:           map[string]*queue{},
		queueStateMarker: queueStateMarker(c, recorder),
		selector:         newRoundRobinSelector(), // TODO: test the weighted rr selector
		queueIndexer:     newQueueIndexer(),
	}

	var err error
	pluginRegistry := plugins.NewPluginRegistry()

	// add tenant plugin
	co.tenantPlugin = plugins.NewQuotaPlugin(c, recorder).(pkgcoordinator.TenantPlugin)
	if tp, err := newTenantPlugin(c, recorder, pluginRegistry, co.config.TenantPlugin); err != nil {
		log.Error(err, "failed to new tenant plugin")
	} else {
		co.tenantPlugin = tp
	}

	// add coordinator plugins
	if err = updatePluginList(c, recorder, &co.prefilterPlugins, pluginRegistry, co.config.PreFilterPlugins); err != nil {
		log.Error(err, "failed to new prefilter plugins")
	}
	if err = updatePluginList(c, recorder, &co.filterPlugins, pluginRegistry, co.config.FilterPlugins); err != nil {
		log.Error(err, "failed to new filter plugins")
	}
	if err = updatePluginList(c, recorder, &co.scorePlugins, pluginRegistry, co.config.ScorePlugins); err != nil {
		log.Error(err, "failed to new score plugins")
	}
	if err = updatePluginList(c, recorder, &co.preDequeuePlugins, pluginRegistry, co.config.PreDequeuePlugins); err != nil {
		log.Error(err, "failed to new pre-dequeue plugins")
	}

	globalCoordinator = co
	go co.Run(context.Background())

	return globalCoordinator
}

// queueStateMarker returns a function to update the job (QueueUnit) status.
func queueStateMarker(c client.Client, recorder record.EventRecorder) func(qu *pkgcoordinator.QueueUnit, reason string) error {
	return func(qu *pkgcoordinator.QueueUnit, reason string) error {
		old := qu.Job.DeepCopyObject()

		msg := fmt.Sprintf("Job %s is queuing and waiting for being scheduled.", qu.Key())
		if reason == utils.JobDequeuedReason {
			msg = fmt.Sprintf("Job %s is being dequeued and waiting for reconciling.", qu.Key())
		}
		if err := utils.UpdateJobConditions(qu.JobStatus, commonapis.JobQueuing, reason, msg); err != nil {
			return err
		}
		recorder.Event(qu.Job, corev1.EventTypeNormal, reason, msg)

		return c.Status().Patch(context.Background(), qu.Job, client.MergeFrom(old.(client.Object)))
	}
}

// newTenantPlugin creates a tenant plugin instance for the coordinator.
func newTenantPlugin(c client.Client, recorder record.EventRecorder, registry plugins.Registry, enabledPlugin string) (pkgcoordinator.TenantPlugin, error) {
	fact, ok := registry[enabledPlugin]
	if !ok {
		return nil, fmt.Errorf("%s does not exist", enabledPlugin)
	}

	p := fact(c, recorder)
	if !reflect.TypeOf(p).Implements(reflect.TypeOf((*pkgcoordinator.TenantPlugin)(nil)).Elem()) {
		return nil, fmt.Errorf("plugin %q does not extend TenantPlugin plugin", enabledPlugin)
	}

	return p.(pkgcoordinator.TenantPlugin), nil
}

// updatePluginList creates the provided plugins instance for the coordinator.
func updatePluginList(c client.Client, recorder record.EventRecorder, pluginList interface{},
	registry plugins.Registry, enabledPlugins []string) error {

	if len(enabledPlugins) == 0 {
		return nil
	}

	existPlugins := reflect.ValueOf(pluginList).Elem()
	pluginType := existPlugins.Type().Elem()

	pSet := sets.NewString()
	for _, pName := range enabledPlugins {
		fact, ok := registry[pName]
		if !ok {
			return fmt.Errorf("%s %q does not exist", pluginType.Name(), pName)
		}

		p := fact(c, recorder)
		if !reflect.TypeOf(p).Implements(pluginType) {
			return fmt.Errorf("plugin %q does not extend %s plugin", pName, pluginType.Name())
		}
		if pSet.Has(pName) {
			return fmt.Errorf("plugin %q already registered as %q", pName, pluginType.Name())
		}

		pSet.Insert(pName)
		newPlugins := reflect.Append(existPlugins, reflect.ValueOf(p))
		existPlugins.Set(newPlugins)
	}

	return nil
}

type Coordinator struct {
	config pkgcoordinator.CoordinateConfiguration

	// lock protects fields down below when concurrent reads/writes happen.
	lock sync.RWMutex

	// queues represents a slice of tenant sub-queues who holds kinds of jobs belongs to one tenant
	// and waiting to be scheduled.
	// tenant is partitioned by TenantPlugin implementation, for example, queue can be partitioned
	// by quota, and each quota represents an independent tenant.
	queues map[string]*queue

	// queueIndexer knows how to find target queue index in slice by object uid or queue name.
	queueIndexer *queueIndexer

	// selector knows how to select next queue.
	selector queueSelector

	// queueStateMarker patches job-in-queue as status queueing with enqueued/dequeued reason.
	queueStateMarker func(qu *pkgcoordinator.QueueUnit, reason string) error

	// plugins embedded in coordinator and called in extensions points.
	tenantPlugin      pkgcoordinator.TenantPlugin
	prefilterPlugins  []pkgcoordinator.PreFilterPlugin
	filterPlugins     []pkgcoordinator.FilterPlugin
	scorePlugins      []pkgcoordinator.ScorePlugin
	preDequeuePlugins []pkgcoordinator.PreDequeuePlugin
}

// EnqueueOrUpdate enqueues the given QueueUnit instance into the coordinator queues if the specified queue exists.
// If the queue is non-exist, create the queue; If the QueueUnit has already been in the queue, update it.
func (co *Coordinator) EnqueueOrUpdate(qu *pkgcoordinator.QueueUnit) {
	qu.Tenant = co.tenantPlugin.TenantName(qu)
	co.initNewQueue(qu)
	if err := co.queueStateMarker(qu, utils.JobEnqueuedReason); err != nil {
		log.Error(err, "failed to update queue unit as queuing with enqueued reason")
	}
	co.addNewQueueUnit(qu.Tenant, qu)
	log.Info("queue unit successfully enqueued", "queue name", qu.Tenant, "key", qu.Key())
}

// initNewQueue initializes a new queue for the given QueueUnit instance if not initialized.
func (co *Coordinator) initNewQueue(qu *pkgcoordinator.QueueUnit) {
	co.lock.Lock()
	defer co.lock.Unlock()

	if _, ok := co.queues[qu.Tenant]; !ok {
		co.queues[qu.Tenant] = newQueue(qu.Tenant)
		log.V(2).Info("new queue created", "queue name", qu.Tenant)
	}
	co.queueIndexer.insert(qu.Job.GetUID(), qu.Tenant)
}

// addNewQueueUnit enqueues the given QueueUnit into the queue identified by tenant.
func (co *Coordinator) addNewQueueUnit(tenant string, qu *pkgcoordinator.QueueUnit) {
	co.lock.Lock()
	defer co.lock.Unlock()

	co.queues[tenant].add(qu)
}

// Dequeue dequeues a QueueUnit instance from the coordinator queues to reconcile.
func (co *Coordinator) Dequeue(uid types.UID) {
	qu := co.popQueueUnitFromQueue(uid)
	if qu == nil || qu.Job == nil {
		return
	}

	// Construct the reconcile request and add it to workqueue.
	// In the words, when the job is dequeued, it is waiting for
	// being scheduled and reconciled immediately.
	qu.Owner.Add(ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: qu.Job.GetNamespace(),
			Name:      qu.Job.GetName(),
		},
	})

	// update job status
	if err := co.queueStateMarker(qu, utils.JobDequeuedReason); err != nil {
		log.Error(err, "failed to update queue unit status to queuing with dequeued reason")
	}

	log.Info("queue unit successfully dequeued", "queue name", qu.Tenant, "key", qu.Key())
}

// popQueueUnitFromQueue pops the QueueUnit instance of the given uid.
func (co *Coordinator) popQueueUnitFromQueue(uid types.UID) *pkgcoordinator.QueueUnit {
	co.lock.Lock()
	defer co.lock.Unlock()

	// firstly, find which queue the job is enqueued
	tenant, exist := co.queueIndexer.lookup(uid)
	if !exist {
		log.Info("[Dequeue] queue unit has already been dequeued, ignore it", "uid", uid)
		return nil
	}

	// fetch the QueueUnit instance
	qu := co.queues[tenant].get(uid)

	// remove it from queues and queueIndexer
	co.queues[tenant].remove(uid)
	co.queueIndexer.remove(uid)

	return qu
}

// SetQueueUnitOwner sets the owner workqueue for the given QueueUnit identified by uid.
func (co *Coordinator) SetQueueUnitOwner(uid types.UID, owner workqueue.RateLimitingInterface) {
	co.lock.Lock()
	defer co.lock.Unlock()

	// firstly, find which queue the job is enqueued
	tenant, ok := co.queueIndexer.lookup(uid)
	if !ok {
		log.Info("[SetQueueUnitOwner] can not find queue, ignore it", "uid", uid)
		return
	}

	// fetch the QueueUnit instance
	qu := co.queues[tenant].get(uid)

	// set the owner
	qu.Owner = owner
	co.queues[tenant].update(qu)
}

// IsQueuing checks the given object is queuing or not.
func (co *Coordinator) IsQueuing(uid types.UID) bool {
	co.lock.RLock()
	defer co.lock.RUnlock()

	tenant, ok := co.queueIndexer.lookup(uid)
	if !ok {
		return false
	}
	return co.queues[tenant].exists(uid)
}

// Run periodically starts the schedule function until the given ctx is done.
func (co *Coordinator) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, co.schedule, co.config.SchedulingPeriod)
}

// schedule popped a selected QueueUnit instance for schedule from a selected queue.
func (co *Coordinator) schedule(ctx context.Context) {
	// select a queue (we provide rr and wrr algorithms for queue selection)
	tenant, q := co.nextQueueToSchedule()
	if q == nil {
		log.V(6).Info("no queue available yet, wait for next request")
		return
	}

	log.V(2).Info("start to schedule next queue", "tenant", tenant, "queue size", q.size())

	// get a QueueUnit iterator
	iter := co.queueSnapshot(q)
	candidateQueueUnits := make([]pkgcoordinator.QueueUnitScore, 0, q.size())
	for iter.HasNext() {
		// get a QueueUnit
		qu := iter.Next()
		if qu.Owner == nil {
			// Owner queue pointer has not been assigned yet due to schedule triggers before SetQueueUnitOwner
			// invoked, since they are running in parallel goroutines. In this case, skip this qu.
			continue
		}

		log.V(3).Info("start to schedule job in queue", "job", qu.Key(), "tenant", tenant)

		// update the job status to enqueued if the status not set yet
		if !utils.IsEnqueued(*qu.JobStatus) {
			if err := co.queueStateMarker(qu, utils.JobEnqueuedReason); err != nil {
				log.Error(err, "failed to update queue unit status to queuing with enqueued reason")
			}
		}

		// check the QueueUnit can be scheduled or not, only passed QueueUnit can be scheduled and reconciled
		if co.isQueueUnitAcceptable(ctx, qu) {
			// scoring the QueueUnit
			quScore := co.prioritizeQueueUnit(ctx, qu)
			candidateQueueUnits = append(candidateQueueUnits, quScore)
		}
	}

	if len(candidateQueueUnits) == 0 {
		log.V(5).Info("empty feasible queue unit after filtering and scoring, scheduling cycle is being interrupted",
			"tenant", tenant)
		return
	}

	// select exact one QueueUnit from the candidates
	selected := co.selectQueueUnit(candidateQueueUnits)
	if len(co.preDequeuePlugins) > 0 {
		for _, p := range co.preDequeuePlugins {
			p.PreDequeue(ctx, selected.QueueUnit)
		}
	}

	// finally, dequeue it for reconciliation
	log.V(3).Info("selected queue unit to be dequeued and reconciled", "key", selected.QueueUnit.Key(), "score", selected.Score)
	co.Dequeue(selected.QueueUnit.Job.GetUID())
}

// nextQueueToSchedule returns a queue for selecting QueueUnits.
// The queue is selected based on round-robin in default.
func (co *Coordinator) nextQueueToSchedule() (string, *queue) {
	qName, q := co.selector.Next(co.queues)
	if q != nil {
		// count the number of pending jobs in this queue
		coordinatorQueuePendingJobsCount.WithLabelValues(qName).Set(float64(q.size()))
	}
	return qName, q
}

// queueSnapshot returns a QueueUnit iterator on the given queue in a thread-safe way.
func (co *Coordinator) queueSnapshot(q *queue) *queueIterator {
	co.lock.RLock()
	defer co.lock.RUnlock()

	return q.iter()
}

// isQueueUnitAcceptable runs all registered (pre & non-pre) filter plugins to check whether the given QueueUnit
// instance can be scheduled in current scheduling circle.
func (co *Coordinator) isQueueUnitAcceptable(ctx context.Context, qu *pkgcoordinator.QueueUnit) bool {
	if len(co.filterPlugins) == 0 {
		return true
	}

	feasible := true

	// run pre-filter plugins
	for _, p := range co.prefilterPlugins {
		log.V(3).Info("run pre filter plugin for queue unit", "plugin name", p.Name(), "key", qu.Key())
		stts := p.PreFilter(ctx, qu)
		if !stts.IsSuccess() {
			if stts.Code() == pkgcoordinator.Skip {
				log.V(3).Info("skip queue unit", "key", qu.Key(), "reason", stts.Reasons())
				continue
			}
			log.V(3).Info("[PreFilter] queue unit is not feasible for current scheduling cycle",
				"reason", stts.Message())
			feasible = false
		}
	}
	if !feasible {
		return false
	}

	// run filter plugins
	for _, p := range co.filterPlugins {
		log.V(3).Info("run filter plugin for queue unit", "plugin name", p.Name(), "key", qu.Key())
		stts := p.Filter(ctx, qu)
		if !stts.IsSuccess() {
			if stts.Code() == pkgcoordinator.Skip {
				log.V(3).Info("skip queue unit", "key", qu.Key(), "reason", stts.Reasons())
				continue
			}
			log.V(3).Info("[Filter] queue unit is not feasible for current scheduling cycle",
				"reason", stts.Message())
			feasible = false
		}
	}

	return feasible
}

// prioritizeQueueUnit runs all registered score plugins and accumulates an integer indicating
// the rank of the given QueueUnit instance.
func (co *Coordinator) prioritizeQueueUnit(ctx context.Context, qu *pkgcoordinator.QueueUnit) pkgcoordinator.QueueUnitScore {
	qus := pkgcoordinator.QueueUnitScore{QueueUnit: qu}
	if len(co.scorePlugins) == 0 {
		return qus
	}

	// run all registered score plugins to get the QueueUnit scores
	for _, p := range co.scorePlugins {
		log.V(3).Info("run score plugin for queue unit", "plugin name", p.Name(), "key", qu.Key())
		score, stts := p.Score(ctx, qu)
		if !stts.IsSuccess() {
			log.V(3).Info("queue unit does not fit current scheduling cycle", "reason", stts.Message())
			continue
		}
		qus.Score += score
	}

	return qus
}

// selectQueueUnit selects a QueueUnit instance with the highest score to be dequeued, if more than
// one QueueUnit instance gets highest score, it will return a randomly selected one.
func (co *Coordinator) selectQueueUnit(candidates []pkgcoordinator.QueueUnitScore) pkgcoordinator.QueueUnitScore {
	maxScore := candidates[0].Score
	selectedIdx := 0
	cntOfMaxScore := 1

	for cIdx, c := range candidates[1:] {
		index := cIdx + 1
		if c.Score > maxScore {
			maxScore = c.Score
			selectedIdx = index
			cntOfMaxScore = 1
		} else if c.Score == maxScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				selectedIdx = index
			}
		}
	}

	return candidates[selectedIdx]
}

/* Queue indexer. */

func newQueueIndexer() *queueIndexer {
	return &queueIndexer{
		data: map[types.UID]string{},
	}
}

// queueIndexer is map from job (QueueUnit) to the queue (tenant) name it is enqueued.
type queueIndexer struct {
	data map[types.UID]string
}

// lookup checks the queue of the given job exists or not.
func (qi *queueIndexer) lookup(uid types.UID) (string, bool) {
	tenant, ok := qi.data[uid]
	return tenant, ok
}

// insert inserts the given job to the queue identified by tenant name.
func (qi *queueIndexer) insert(uid types.UID, tenant string) {
	if qi.data == nil {
		qi.data = make(map[types.UID]string)
	}
	qi.data[uid] = tenant
}

// remove removes the job entry from the map.
// This is triggered when the job is dequeued.
func (qi *queueIndexer) remove(uid types.UID) {
	delete(qi.data, uid)
}
