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

package train

import (
	"context"
	"fmt"
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/controllers/common"
	"github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	coordinatorcore "github.com/hliangzhao/torch-on-k8s/pkg/coordinator/core"
	"github.com/hliangzhao/torch-on-k8s/pkg/features"
	gangschedulerregistry "github.com/hliangzhao/torch-on-k8s/pkg/gangscheduler/registry"
	"github.com/hliangzhao/torch-on-k8s/pkg/metrics"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils"
	"github.com/hliangzhao/torch-on-k8s/pkg/utils/kubeconfig"
	patchutils "github.com/hliangzhao/torch-on-k8s/pkg/utils/patch"
	kruisev1alpha1 "github.com/openkruise/kruise/apis/apps/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"math"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strconv"
	"strings"
)

// SetupWithManager sets up the controller with the Manager.
func (r *TorchJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// create the controller with the torchjob reconciler
	c, err := controller.New(r.ControllerName(), mgr, controller.Options{
		Reconciler:              r,
		MaxConcurrentReconciles: common.JobControllerConfig.MaxNumConcurrentReconciles,
	})
	if err != nil {
		return err
	}

	// Watch owner resource with create/delete/update event filter.
	if err = c.Watch(&source.Kind{Type: &trainv1alpha1.TorchJob{}}, &handler.EnqueueRequestForObject{}, predicate.Funcs{
		CreateFunc: common.OnOwnerCreateFunc(r.Scheme, trainv1alpha1.ExtractMetaFieldsFromObject, log, r.coordinator, r.jobController.Metrics),
		UpdateFunc: common.OnOwnerUpdateFunc(r.Scheme, trainv1alpha1.ExtractMetaFieldsFromObject, log, r.coordinator),
		DeleteFunc: common.OnOwnerDeleteFunc(r.jobController, trainv1alpha1.ExtractMetaFieldsFromObject, log),
	}); err != nil {
		return err
	}

	// Watch managed resources (pods & services) with owner and create/delete/update event filter.
	if err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &trainv1alpha1.TorchJob{},
		IsController: true,
	}, predicate.Funcs{
		CreateFunc: r.jobController.OnPodCreateFunc,
		UpdateFunc: r.jobController.OnPodUpdateFunc,
		DeleteFunc: r.jobController.OnPodDeleteFunc,
	}); err != nil {
		return err
	}

	if err = c.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{
		OwnerType:    &trainv1alpha1.TorchJob{},
		IsController: true,
	}, predicate.Funcs{
		CreateFunc: r.jobController.OnServiceCreateFunc,
		UpdateFunc: r.jobController.OnServiceUpdateFunc,
		DeleteFunc: r.jobController.OnServiceDeleteFunc,
	}); err != nil {
		return err
	}

	// We use kruise to inplace restart pods when failover is performed.
	// Thus, we need to monitor the related events.
	// Check the kruise api is enabled or not firstly.
	if enabled := isWorkloadCRDInstalled(&kruisev1alpha1.ContainerRecreateRequest{}, mgr.GetScheme()); enabled {
		if err = c.Watch(&source.Kind{Type: &kruisev1alpha1.ContainerRecreateRequest{}}, &handler.EnqueueRequestForOwner{
			OwnerType:    &trainv1alpha1.TorchJob{},
			IsController: false,
		}); err != nil {
			return err
		}
	}

	return nil
}

const (
	controllerName = "TorchJobController"
)

var (
	log                            = logf.Log.WithName("torchjob-controller")
	_   reconcile.Reconciler       = &TorchJobReconciler{}
	_   common.ControllerInterface = &TorchJobReconciler{}
)

func NewTorchJobReconciler(manager ctrl.Manager, config common.JobControllerConfiguration) *TorchJobReconciler {
	r := &TorchJobReconciler{
		Client: manager.GetClient(),
		Scheme: manager.GetScheme(),
	}
	r.recorder = manager.GetEventRecorderFor(r.ControllerName())
	r.jobController = common.NewJobController(manager, r, config, r.recorder,
		metrics.NewJobMetrics(trainv1alpha1.TorchJobKind, r.Client), manager.GetScheme())

	// TODO: Why set gang scheduling in config while coordinator in feature gate?
	if r.jobController.Config.EnableGangScheduling {
		r.jobController.GangScheduler = gangschedulerregistry.Get(r.jobController.GangScheduler.SchedulerName())
	}
	if features.FeatureGates.Enabled(features.JobCoordinator) {
		r.coordinator = coordinatorcore.NewCoordinator(manager)
	}
	return r
}

// TorchJobReconciler reconciles a TorchJob object.
type TorchJobReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	recorder      record.EventRecorder
	jobController common.JobController
	coordinator   coordinator.Coordinator
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// In the Reconcile function, we compare the state specified by the TorchJob object
// against the actual cluster state, and then perform operations to make the cluster
// state reflect the state specified by the user.
//
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create
// +kubebuilder:rbac:groups=scheduling.volcano.sh,resources=podgroups;queues,verbs=*
// +kubebuilder:rbac:groups=train.distributed.io,resources=torchjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=train.distributed.io,resources=torchjobs/status,verbs=get;update;patch
func (r *TorchJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// fetch the existing torchjob resource from cluster
	torchJob := &trainv1alpha1.TorchJob{}
	err := r.Client.Get(context.Background(), req.NamespacedName, torchJob)
	if err != nil {
		if errors.IsNotFound(err) {
			// Since the torchjob resource has been deleted. Here we need to remove the finalizer
			// added by the torchjobs to the controlled pods. Specifically, the `FinalizerPreemptProtector` finalizer.
			// When the finalizer is remove, these pods can be finally deleted by GC.
			log.Info("try to get job but it has been deleted", "key", req.String())
			if err = r.cleanUpPreemptFinalizers(req.Namespace, req.Name); err != nil {
				return ctrl.Result{}, err
			}
			r.jobController.Metrics.DeletedInc()
			return ctrl.Result{}, nil
		}
	}

	// Check the fetched torchjob resource need to be reconciled or not --
	// check whether the expectation status has been achieved or not.
	torchJobCopy := torchJob.DeepCopy()
	needReconcile := r.jobController.SatisfyExpectations(torchJobCopy, torchJobCopy.Spec.TorchTaskSpecs) ||
		r.EnableElasticScaling(torchJobCopy, &torchJobCopy.Spec.RunPolicy)
	if !needReconcile || torchJobCopy.DeletionTimestamp != nil {
		// if no need to do the reconcile, or the job has been marked to be deleted, just do thing and return
		log.Info("reconcile cancelled, job does not need to do reconcile or has been deleted",
			"sync", needReconcile, "deleted", torchJobCopy.DeletionTimestamp != nil)
		return ctrl.Result{}, nil
	}

	// set the untouched fields of the given torchjob spec as default before reconciliation
	r.Scheme.Default(torchJobCopy)

	// entrust the job controller to do the reconciliation
	result, err := r.jobController.ReconcileJobs(torchJobCopy, torchJobCopy.Spec.TorchTaskSpecs, torchJobCopy.Spec.MinMembers,
		torchJobCopy.Status, &torchJobCopy.Spec.RunPolicy, &torchJobCopy.Spec.ModelVersion.Spec)
	if err != nil {
		log.Error(err, "pytorch job reconcile failed")
		return result, err
	}
	return result, nil
}

func (r *TorchJobReconciler) ControllerName() string {
	return controllerName
}

func (r *TorchJobReconciler) GetAPIGroupVersionKind() schema.GroupVersionKind {
	return trainv1alpha1.SchemeGroupVersion.WithKind(trainv1alpha1.TorchJobKind)
}

func (r *TorchJobReconciler) GetAPIGroupVersion() schema.GroupVersion {
	return trainv1alpha1.SchemeGroupVersion
}

func (r *TorchJobReconciler) GetGroupName() string {
	return trainv1alpha1.SchemeGroupVersion.Group
}

// GetNodeForModelOutput returns the node name on which the trained model
// of the corresponding torch training job will be saved.
func (r *TorchJobReconciler) GetNodeForModelOutput(pods []*corev1.Pod) (nodeName string) {
	// NOTE: I try to replace the node with the result of the best node selection algorithm but failed.
	// The reason is that the if the node is not the node where the pod runs on, the host path is illegal.
	// Thus, just return back to the default node where the master task pod is placed.
	for _, pod := range pods {
		taskType := pod.Labels[trainv1alpha1.LabelTaskType]
		taskIndex, _ := strconv.Atoi(pod.Labels[trainv1alpha1.LabelTaskIndex])
		if taskType == strings.ToLower(string(trainv1alpha1.TaskTypeTorchMaster)) && taskIndex == 0 {
			log.Info("select %s for model output", pod.Spec.NodeName)
			return pod.Spec.NodeName
		}
	}
	log.Info("no master task type, select node %s for model output", pods[0].Spec.NodeName)
	return pods[0].Spec.NodeName
}

// getMostAvailableStorageNode returns the node with the most available storage capacity with node affinity considered.
// If at least one node is labeled with "fast for storage", we will select the node with the most available storage capacity.
// Otherwise, we will select the node with the most available storage capacity from all nodes.
// NOTE: This func is temporarily deprecated.
func getMostAvailableStorageNode() (string, error) {
	var (
		mostAvailNode string
		maxAvail      float64 = -1
	)

	// create an in-cluster client to fetch the node info
	config, err := kubeconfig.GetClusterConfig()
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}

	// consider the nodes with label tagged first
	nodeSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{modelv1alpha1.LabelNodeStorageType: modelv1alpha1.LabelNodeStorageTypeFast},
	}
	nodeList, err := clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set(nodeSelector.MatchLabels).String(),
	})
	if err != nil {
		return "", err
	}

	// If no nodes match the node affinity, get all nodes in the cluster
	if len(nodeList.Items) == 0 {
		nodeList, err = clientset.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return "", err
		}
	}

	// select the one with the largest available storage capacity
	for _, node := range nodeList.Items {
		capacityStr := node.Status.Capacity.StorageEphemeral().String()
		capacity, err := resource.ParseQuantity(capacityStr)
		if err != nil {
			return "", err
		}
		usedStr := node.Status.Allocatable.StorageEphemeral().String()
		used, err := resource.ParseQuantity(usedStr)
		if err != nil {
			return "", err
		}
		available := capacity.Value() - used.Value()
		availableStr := strconv.FormatFloat(math.Round(float64(available)/float64(1<<30)*100)/100, 'f', -1, 64)
		availableFloat, err := strconv.ParseFloat(availableStr, 64)
		if err != nil {
			return "", err
		}

		if availableFloat > maxAvail {
			maxAvail = availableFloat
			mostAvailNode = node.GetName()
		}
	}

	return mostAvailNode, nil
}

// SetClusterSpec sets the containers details (such as env var, character, ect) according to the torchjob spec.
func (r *TorchJobReconciler) SetClusterSpec(ctx context.Context, job interface{}, podTemplate *corev1.PodTemplateSpec,
	taskType, taskIndex string) error {

	torchJob, ok := job.(*trainv1alpha1.TorchJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of TorchJob", job)
	}
	rank, err := strconv.Atoi(taskIndex)
	if err != nil {
		return err
	}
	masterPort, err := getPortFromJob(torchJob.Spec.TorchTaskSpecs,
		trainv1alpha1.TaskTypeTorchMaster, trainv1alpha1.TorchJobDefaultContainerName, trainv1alpha1.TorchJobDefaultPortName)
	if err != nil {
		return err
	}

	// set network mode
	masterRole := taskType == strings.ToLower(string(trainv1alpha1.TaskTypeTorchMaster))
	if masterHostPort, ok := common.GetHostNetworkPortFromContext(ctx, "master", "0"); common.EnableHostNetwork(torchJob) && ok {
		if masterRole || features.FeatureGates.Enabled(features.HostNetWithHeadlessSvc) {
			masterPort = masterHostPort
		}
	}
	masterAddr := utils.GenGeneralName(torchJob.Name, strings.ToLower(string(trainv1alpha1.TaskTypeTorchMaster)), strconv.Itoa(0))
	if masterRole {
		if rank != 0 {
			return fmt.Errorf("invalid config: There should be only a single master with index=0")
		}
		if features.FeatureGates.Enabled(features.TorchLocalMasterAddr) {
			masterAddr = "localhost"
		}
	} else {
		rank++
	}

	numTotalTasks := int(utils.GetTotalExcludedTasks(torchJob.Spec.TorchTaskSpecs, trainv1alpha1.TaskTypeAIMaster))
	enableElasticScaling := torchJob.Annotations[trainv1alpha1.AnnotationEnableElasticTraining] == "true"
	if enableElasticScaling && !masterRole && taskType != "aimaster" {
		AddImageWarmupForWorker(podTemplate, r.GetDefaultContainerPortName())
		err = AddMasterWaiterForWorker(podTemplate, InitContainerParam{
			MasterAddr:         masterAddr,
			InitContainerImage: "docker.io/alpine:3.10",
		})
		if err != nil {
			return err
		}
	}

	// Set the torchelastic args as the torchelastic policy indicates.
	desiredReplicas, err := getDesiredReplicas(torchJob)
	if err != nil {
		return err
	}
	// read the settings from the elastic policy
	var numMinReplicas, numMaxReplicas, numWorkersPerNode int32
	if torchJob.Spec.TorchElasticPolicy.NumMinReplicas != nil {
		numMinReplicas = *torchJob.Spec.TorchElasticPolicy.NumMinReplicas
	} else {
		numMinReplicas = desiredReplicas
	}
	if torchJob.Spec.TorchElasticPolicy.NumMaxReplicas != nil {
		numMaxReplicas = *torchJob.Spec.TorchElasticPolicy.NumMaxReplicas
	} else {
		numMaxReplicas = desiredReplicas
	}
	if torchJob.Spec.TorchElasticPolicy.NProcPerNode != nil {
		numWorkersPerNode = *torchJob.Spec.TorchElasticPolicy.NProcPerNode
	} else {
		numWorkersPerNode = int32(1)
	}
	// generate torchelastic args
	// (https://pytorch.org/docs/stable/elastic/quickstart.html)
	torchelasticArgs := []string{
		"--rdzv_backend=" + torchJob.Spec.TorchElasticPolicy.RendezvousBackend,
		"--rdzv_endpoint=" + torchJob.Spec.TorchElasticPolicy.RendezvousEndpoint,
		"--rdzv_id=" + torchJob.Name,
		"--nproc_per_node=" + strconv.Itoa(int(numWorkersPerNode)),
		"--nnodes=" + strconv.Itoa(int(numMinReplicas)) + ":" + strconv.Itoa(int(numMaxReplicas))}

	for i := range podTemplate.Spec.Containers {
		if len(podTemplate.Spec.Containers[i].Env) == 0 {
			podTemplate.Spec.Containers[i].Env = make([]corev1.EnvVar, 0)
		}
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  "MASTER_PORT",
			Value: strconv.Itoa(int(masterPort)),
		})
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  "MASTER_ADDR",
			Value: masterAddr,
		})
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  "RANK",
			Value: strconv.Itoa(rank),
		})
		podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  "PYTHONUNBUFFERED",
			Value: "0",
		})

		if torchJob.Spec.EnableTorchElastic && torchJob.Spec.TorchElasticPolicy != nil {
			podTemplate.Spec.Containers[i].Args = append(torchelasticArgs, podTemplate.Spec.Containers[i].Args...)
		}

		if enableElasticScaling && taskType != "aimaster" {
			// Job enables elastic scaling select value of AnnotationWorldSize as its
			// WORLD_SIZE env value via field-path, the annotated value will be mutated
			// during scaling progress and processes in container will acknowledge the
			// updated world-size value after restarted.
			if podTemplate.Annotations == nil {
				podTemplate.Annotations = make(map[string]string)
			}
			podTemplate.Annotations[AnnotationWorldSize] = strconv.Itoa(numTotalTasks)

			podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
				Name: "WORLD_SIZE",
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: fmt.Sprintf("metadata.annotations['%s']", AnnotationWorldSize),
				}},
			})

			// Overwrite restart policy as OnFailure so that container will be started again
			// by kubelet after kruise daemon set forcefully execute restarting container through
			// CRI calls bypasses kubelet.
			podTemplate.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		} else {
			podTemplate.Spec.Containers[i].Env = append(podTemplate.Spec.Containers[i].Env, corev1.EnvVar{
				Name:  "WORLD_SIZE",
				Value: strconv.Itoa(numTotalTasks),
			})
		}
	}

	return nil
}

func (r *TorchJobReconciler) GetDefaultContainerName() string {
	return trainv1alpha1.TorchJobDefaultContainerName
}

func (r *TorchJobReconciler) GetDefaultContainerPortName() string {
	return trainv1alpha1.TorchJobDefaultPortName
}

func (r *TorchJobReconciler) GetDefaultContainerPortNumber() int32 {
	return trainv1alpha1.TorchJobDefaultPort
}

// GetTaskReconcilerOrders sets the default task starting & running orders (based on the DAG condition).
func (r *TorchJobReconciler) GetTaskReconcilerOrders() []trainv1alpha1.TaskType {
	return []trainv1alpha1.TaskType{
		// the dependency relationship: AIMAster --> Master --> Worker
		trainv1alpha1.TaskTypeAIMaster,
		trainv1alpha1.TaskTypeTorchMaster,
		trainv1alpha1.TaskTypeTorchWorker,
	}
}

// IsMaster checks the given taskType is Master or not.
func (r *TorchJobReconciler) IsMaster(tasks map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, taskType trainv1alpha1.TaskType) bool {
	_, ok := tasks[trainv1alpha1.TaskTypeTorchMaster]
	return ok && taskType == trainv1alpha1.TaskTypeTorchMaster
}

// cleanUpPreemptFinalizers cleans up the `FinalizerPreemptProtector` finalizer in pods meta for a deleted torchjob.
func (r *TorchJobReconciler) cleanUpPreemptFinalizers(namespace, name string) error {
	// fetch the to-be-cleanup pods
	selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: r.jobController.GenerateLabels(name),
	})

	pods := corev1.PodList{}
	if err := r.Client.List(context.Background(), &pods, client.InNamespace(namespace),
		client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return err
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if utils.HasFinalizer(pod.Finalizers, trainv1alpha1.FinalizerPreemptProtector) {
			klog.V(2).Infof("pod %s has finalizer %s, need to remove", pod.Name, trainv1alpha1.FinalizerPreemptProtector)
			patch := patchutils.NewStrategicPatch()
			patch.RemoveFinalizer(trainv1alpha1.FinalizerPreemptProtector)
			if err := r.Client.Patch(context.Background(), pod, patch); err != nil {
				return err
			}
		}
	}

	return nil
}

// getPortFromJob gets the port of job default container.
func getPortFromJob(spec map[trainv1alpha1.TaskType]*trainv1alpha1.TaskSpec, taskType trainv1alpha1.TaskType, containerName, portName string) (int32, error) {
	containers := spec[taskType].Template.Spec.Containers
	for _, container := range containers {
		if container.Name == containerName {
			ports := container.Ports
			for _, port := range ports {
				if port.Name == portName {
					return port.ContainerPort, nil
				}
			}
		}
	}
	return -1, fmt.Errorf("failed to found the port")
}

// getDesiredReplicas returns the number of worker replicas for the given torchjob.
func getDesiredReplicas(job *trainv1alpha1.TorchJob) (int32, error) {
	masterTask, ok := job.Spec.TorchTaskSpecs[trainv1alpha1.TaskTypeTorchMaster]
	if !ok {
		return 0, fmt.Errorf("torchjob %v does not have %v", job, trainv1alpha1.TaskTypeTorchMaster)
	}
	return *masterTask.NumTasks, nil
}

// isWorkloadCRDInstalled checks whether the given CRD is installed or not.
func isWorkloadCRDInstalled(workload runtime.Object, scheme *runtime.Scheme) bool {
	gvk, err := apiutil.GVKForObject(workload, scheme)
	if err != nil {
		klog.Warningf("unrecognized CustomResource object %+v in scheme: %v", gvk, err)
		return false
	}

	discoveryClient := discovery.NewDiscoveryClientForConfigOrDie(ctrl.GetConfigOrDie())
	crdList, err := discoveryClient.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		klog.Warningf("workload CRD %s not found in discovery, err: %s", gvk, err)
		return false
	}
	for _, crd := range crdList.APIResources {
		if crd.Kind == gvk.Kind {
			klog.Infof("workload CRD %s found.", crd.Kind)
			return true
		}
	}
	return false
}
