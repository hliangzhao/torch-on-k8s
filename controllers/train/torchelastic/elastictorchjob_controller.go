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

package torchelastic

import (
	"context"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils/concurrent"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strings"
	"sync"
	"time"
)

const (
	controllerName = "TorchJobController"

	interval        = 5 * time.Second
	podReadyTimeout = 1 * time.Minute
)

var (
	log                                               = logf.Log.WithName("elastictorchjob-controller")
	elastictorchjobControllerMap                      = make(map[runtime.Object]newElasticTorchJobController)
	_                            reconcile.Reconciler = &ElasticTorchJobReconciler{}
)

func init() {
	elastictorchjobControllerMap[&trainv1alpha1.TorchJob{}] = NewElasticTorchJobReconciler
}

func SetupWithManager(mgr ctrl.Manager) error {
	r := NewElasticTorchJobReconciler(mgr, 30, 5)
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}
	return nil
}

func NewElasticTorchJobReconciler(mgr ctrl.Manager, period, count int) ElasticTorchJobController {
	return &ElasticTorchJobReconciler{
		period:      period,
		metricCount: count,
		client:      kubernetes.NewForConfigOrDie(mgr.GetConfig()),
		Client:      mgr.GetClient(),
		metrics:     map[string]map[int32][]MetricObservation{},
		jobs:        map[string]ElasticTorchJob{},
	}
}

type ElasticTorchJobReconciler struct {
	// TODO: figure out the best place for period and metricCount
	period      int                   // elastic scaling loop
	metricCount int                   // the epoch counting time
	client      *kubernetes.Clientset // TODO: this can be removed
	client.Client
	mutex sync.Mutex

	// metrics is: {jobName: {numReplicas: a list of observations}}
	metrics map[string]map[int32][]MetricObservation
	jobs    map[string]ElasticTorchJob
}

// ElasticTorchJob is a wrapper of a torchjob which implements native torchelastic.
type ElasticTorchJob struct {
	Name       string
	Namespace  string
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// MetricObservation collects the observation in a training epoch.
type MetricObservation struct {
	Epoch    int32   `json:"epoch,omitempty"`
	Batch    int32   `json:"batch,omitempty"`
	Accuracy float64 `json:"accuracy,omitempty"`
	Latency  float64 `json:"latency,omitempty"`
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// In the Reconcile function, we compare the state specified by the TorchJob object
// against the actual cluster state, and then perform operations to make the cluster
// state reflect the state specified by the user.
func (r *ElasticTorchJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	torchjob := trainv1alpha1.TorchJob{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{
		Namespace: req.Namespace,
		Name:      req.Name,
	}, &torchjob); err != nil {
		if errors.IsNotFound(err) {
			// TODO: What about finalizers?
			log.Info("try to get job but it has been deleted", "key", req.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ElasticTorchJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := controller.New(controllerName, mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}
	if err = c.Watch(&source.Kind{Type: &trainv1alpha1.TorchJob{}}, &handler.EnqueueRequestForObject{}, predicate.Funcs{
		CreateFunc: OnOwnerCreateFunc(r),
		DeleteFunc: OnOwnerDeleteFunc(r),
	}); err != nil {
		return err
	}

	ctx := context.Background()
	// TODO: This is similar to coordinator. Re-write torchelastic in the coordinator way.
	go wait.UntilWithContext(ctx, r.StartElasticScalingLoop, time.Duration(r.period)*time.Second)
	klog.Infof("start torchelastic scaling loop")
	ctx.Done()

	klog.Infof("shutdown torchelastic scaling controller loop")
	return nil
}

func (r *ElasticTorchJobReconciler) StartElasticScalingLoop(ctx context.Context) {
	tickets := 100
	if len(r.jobs) < 100 {
		tickets = len(r.jobs)
	}

	sema := concurrent.NewSemaphore(tickets)
	for _, job := range r.jobs {
		sema.Acquire()

		go func(job *ElasticTorchJob) {
			defer sema.Release()
			r.start(job.ctx, job.cancelFunc, job.Name, job.Namespace)
		}(&job)
	}
	sema.Wait()
}

func (r *ElasticTorchJobReconciler) GenerateLabels(jobName string) map[string]string {
	return map[string]string{
		trainv1alpha1.LabelGroupName: r.GetGroupName(),
		trainv1alpha1.LabelJobName:   strings.Replace(jobName, "/", "-", -1),
	}
}

func (r *ElasticTorchJobReconciler) ControllerName() string {
	return controllerName
}

func (r *ElasticTorchJobReconciler) GetGroupName() string {
	return trainv1alpha1.SchemeGroupVersion.Group
}
