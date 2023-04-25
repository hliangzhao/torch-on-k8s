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

package model

import (
	"context"
	"fmt"
	trainv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/train/v1alpha1"
	"github.com/hliangzhao/torch-on-k8s/controllers/common"
	"github.com/hliangzhao/torch-on-k8s/pkg/storage/registry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
)

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	var predicates = predicate.Funcs{
		// we only care about the Create event of a modelversion resource
		CreateFunc: func(e event.CreateEvent) bool {
			mv := e.Object.(*modelv1alpha1.ModelVersion)
			if mv.DeletionTimestamp != nil {
				return false
			}
			return true
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&modelv1alpha1.ModelVersion{}, builder.WithPredicates(predicates)).
		Complete(r)
}

func NewModelVersionReconciler(manager ctrl.Manager, _ common.JobControllerConfiguration) *Reconciler {
	return &Reconciler{
		Client: manager.GetClient(),
		Scheme: manager.GetScheme(),
	}
}

var (
	log                      = logf.Log.WithName("modelversion-controller")
	_   reconcile.Reconciler = &Reconciler{}
)

// Reconciler reconciles a ModelVersion object
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// This reconciliation needs to operate resources modelversion, model, pv and pvc. So, we need to add the rbac for them.
// +kubebuilder:rbac:groups=model.distributed.io,resources=modelversions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=model.distributed.io,resources=modelversions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=model.distributed.io,resources=models,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=model.distributed.io,resources=models/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(_ context.Context, req ctrl.Request) (ctrl.Result, error) {
	// fetch the request modelversion resource from the cluster
	mv := &modelv1alpha1.ModelVersion{}
	err := r.Get(context.Background(), req.NamespacedName, mv)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("modelversion doesn't exist", "name", req.String())
			return reconcile.Result{}, nil
		}
		log.Error(err, "fail to get modelversion "+req.String())
		return reconcile.Result{}, err
	}

	// if no need to build the image, return directly
	if mv.Status.ImageBuildPhase == modelv1alpha1.ImageBuildSucceeded ||
		mv.Status.ImageBuildPhase == modelv1alpha1.ImageBuildFailed {
		log.Info(fmt.Sprintf("image build %s", mv.Status.ImageBuildPhase), "modelversion", mv.Name)
		return reconcile.Result{}, nil
	}

	// Now we start the building procedure. Firstly, we need to create the required necessities to start our work,
	// including the model resource, the related pv and pvc resources.

	// if the corresponding model does not exist, create it
	model := &modelv1alpha1.Model{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: mv.Namespace, Name: mv.Spec.Model}, model)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("create model " + mv.Spec.Model)
			model = &modelv1alpha1.Model{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: mv.Namespace,
					Name:      mv.Spec.Model,
				},
				Spec:   modelv1alpha1.ModelSpec{},
				Status: modelv1alpha1.ModelStatus{},
			}

			// associate the latest ModelVersion to the Model
			model.Status.LatestVersion = &modelv1alpha1.VersionInfo{
				ModelVersion: mv.Name,
			}

			// The modelversion resource `mv` is controlled by this model. Indicate that by setting the owner reference.
			if mv.OwnerReferences == nil {
				mv.OwnerReferences = make([]metav1.OwnerReference, 0)
			}
			exists := false
			for _, ref := range mv.OwnerReferences {
				if ref.Kind == model.Kind && ref.UID == model.UID {
					exists = true
					break
				}
			}
			if !exists {
				mv.OwnerReferences = append(mv.OwnerReferences, metav1.OwnerReference{
					APIVersion: model.APIVersion,
					Kind:       model.Kind,
					Name:       model.Name,
					UID:        model.UID,
				})
			}

			// create the model resource in cluster
			err := r.Create(context.Background(), model)
			if err != nil {
				log.Error(err, "failed to create model "+model.Name)
				return reconcile.Result{Requeue: true}, err
			}
		} else {
			return ctrl.Result{}, err
		}
	}

	// create pv and pvc for the model
	pv := &corev1.PersistentVolume{}
	pvc := &corev1.PersistentVolumeClaim{}

	if mv.Spec.Storage == nil {
		log.Error(err, "storage is undefined", "modelversion", mv.Name)
		return reconcile.Result{}, nil
	} else {
		if err = r.createPVAndPVCForModelVersion(mv, pv, pvc); err != nil {
			log.Error(err, "failed to create pv/pvc", "modelversion", mv.Name)
			return reconcile.Result{Requeue: true}, err
		}
	}

	// check if the pvc is bound to pv
	if pvc.Status.Phase != corev1.ClaimBound {
		// wait for the pv and pvc to be bound
		log.Info("waiting for pv and pvc to be bound", "modelversion", mv.Name, "pv", pv.Name, "pvc", pvc.Name)
		return ctrl.Result{Requeue: true, RequeueAfter: 1 * time.Second}, nil
	}

	// Now, everything about the model is prepared well. Let's start building the model image with Kaniko.

	mvStatus := mv.Status.DeepCopy()

	// create a Kaniko pod to build the model image, take the first 5 chars as the version ID if tag is not provided
	versionID := string(mv.UID[:5])
	if len(mv.Spec.ImageTag) > 0 {
		versionID = mv.Spec.ImageTag
	}
	imgBuildPodName := getBuildImagePodName(model.Name, versionID)
	imgBuildPod := &corev1.Pod{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: model.Namespace, Name: imgBuildPodName}, imgBuildPod)
	if err != nil {
		if errors.IsNotFound(err) {
			mvStatus.Image = mv.Spec.ImageRepo + ":" + versionID
			mvStatus.ImageBuildPhase = modelv1alpha1.ImageBuilding
			mvStatus.Message = "Image building started."

			// create a configmap resource in cluster which includes the dockerfile command
			if err = r.createDockerfileIfNotExists(mv); err != nil {
				return ctrl.Result{}, err
			}

			// create the kaniko pod
			imgBuildPod = createImgBuildPod(mv, pvc, imgBuildPodName, mvStatus.Image)
			if err = r.Create(context.Background(), imgBuildPod); err != nil {
				return ctrl.Result{}, err
			}

			// update modelversion labels and annotations
			if mv.Labels == nil {
				mv.Labels = make(map[string]string, 1)
			}
			mv.Labels[trainv1alpha1.LabelModelName] = model.Name
			if mv.Annotations == nil {
				mv.Annotations = make(map[string]string, 1)
			}
			mv.Annotations[trainv1alpha1.AnnotationImgBuildPodName] = imgBuildPodName

			// update modelversion resource in cluster with the newest created model resource
			mvCopy := mv.DeepCopy()
			mvCopy.Status = *mvStatus
			if err = r.Update(context.Background(), mvCopy); err != nil {
				log.Error(err, "failed to update modelversion", "ModelVersion", mvCopy.Name)
				return ctrl.Result{}, err
			}

			// update model resource in cluster with the newest modelversion resource
			modelCopy := model.DeepCopy()
			modelCopy.Status.LatestVersion = &modelv1alpha1.VersionInfo{
				ModelVersion: mv.Name,
				Image:        mvStatus.Image,
			}
			if err = r.Status().Update(context.Background(), modelCopy); err != nil {
				log.Error(err, "failed to update model", "Model", model.Name)
				return ctrl.Result{}, err
			}

			log.Info(fmt.Sprintf("model %s image build started", mv.Name), "image-build-pod-name", imgBuildPodName)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		} else {
			return ctrl.Result{}, err
		}
	}

	// check image build status and set it to modelversion status
	currentTime := metav1.Now()
	if imgBuildPod.Status.Phase == corev1.PodSucceeded {
		mvStatus.ImageBuildPhase = modelv1alpha1.ImageBuildSucceeded
		mvStatus.Message = fmt.Sprintf("Image build succeeded.")
		mvStatus.FinishTime = &currentTime
		log.Info(fmt.Sprintf("modelversion %s image build succeeded", mv.Name))
	} else if imgBuildPod.Status.Phase == corev1.PodFailed {
		mvStatus.ImageBuildPhase = modelv1alpha1.ImageBuildFailed
		mvStatus.FinishTime = &currentTime
		mvStatus.Message = fmt.Sprintf("Image build failed.")
		log.Info(fmt.Sprintf("modelversion %s image build failed", mv.Name))
	} else {
		// image not ready
		log.Info(fmt.Sprintf("modelversion %s image is building", mv.Name), "image-build-pod-name", imgBuildPodName)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// set the newest modelversion status in cluster
	versionCopy := mv.DeepCopy()
	versionCopy.Status = *mvStatus
	err = r.Update(context.Background(), versionCopy)
	if err != nil {
		log.Error(err, "failed to update modelversion", "ModelVersion", mv.Name)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// createDockerfileIfNotExists creates the dockerfile configmap. The name of the configmap is "dockerfile",
// the data of the configmap is the commands inside the dockerfile. What the commands do is very simple:
// Copy the model artifact from the mounted pvc to some path inside the image. This dockerfile configmap
// can be reused by all the torchjobs. Thus, no model or modelversion specified info is used when creating
// them.
func (r *Reconciler) createDockerfileIfNotExists(modelVersion *modelv1alpha1.ModelVersion) error {
	dockerfile := &corev1.ConfigMap{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: modelVersion.Namespace, Name: "dockerfile"}, dockerfile)
	if err != nil {
		if errors.IsNotFound(err) {
			dockerfile = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dockerfile",
					Namespace: modelVersion.Namespace,
				},
				Data: map[string]string{
					"dockerfile": fmt.Sprintf(
						`FROM busybox
COPY build/ %s`, modelv1alpha1.DefaultModelPathInImage),
				},
			}
			if err = r.Create(context.Background(), dockerfile); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

// createImgBuildPod creates a Kaniko pod to build the model image.
// 1. mount dockerfile configmap into /workspace/dockerfile.
// 2. mount build source pvc into /workspace/build.
// 3. mount dockerconfig secret as /kaniko/.docker/config.json.
// The image build pod will create an image that includes artifacts under "/workspace/build", based on the dockerfile.
func createImgBuildPod(modelVersion *modelv1alpha1.ModelVersion, pvc *corev1.PersistentVolumeClaim,
	imgBuildPodName string, newImage string) *corev1.Pod {

	// create the kaniko pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: modelVersion.Namespace,
			Name:      imgBuildPodName,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  imgBuildPodName,
					Image: common.JobControllerConfig.ModelImageBuilder,
					Args: []string{
						"--skip-tls-verify=true",
						"--dockerfile=/workspace/dockerfile",
						"--context=dir:///workspace/",
						fmt.Sprintf("--destination=%s", newImage)},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	// create volumes for the kaniko pod
	var volumes = []corev1.Volume{
		{
			Name: "kaniko-secret",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: "regcred",
					Items: []corev1.KeyToPath{
						{
							Key:  ".dockerconfigjson",
							Path: "config.json",
						},
					},
				},
			},
		},
		{
			Name: "build-source",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
				},
			},
		},
		{
			Name: "dockerfile",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "dockerfile",
					},
				},
			},
		},
	}
	pod.Spec.Volumes = volumes

	// mount volumes for the kaniko pod
	var volumeMounts = []corev1.VolumeMount{
		{
			Name:      "kaniko-secret", // the docker secret for pushing images
			MountPath: "/kaniko/.docker",
		},
		{
			Name:      "build-source", // build-source references the pvc for the model
			MountPath: "/workspace/build",
		},
		{
			Name:      "dockerfile", // dockerfile is the default Dockerfile for building the model image
			MountPath: "/workspace/",
		},
	}
	pod.Spec.Containers[0].VolumeMounts = volumeMounts

	// the kaniko pod is controlled by the modelversion resource
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{
		APIVersion: modelVersion.APIVersion,
		Kind:       modelVersion.Kind,
		Name:       modelVersion.Name,
		UID:        modelVersion.UID,
	})

	return pod
}

// createPVAndPVCForModelVersion creates pv and its pvc to be bound in cluster for the given modelversion.
// LocalStorage is treated different from remote storage. For local storage, each node will be provisioned
// with its own pv and pvc, and append nodeName as the pv/pvc name, whereas remote storage only has a single
// pv and pvc.
func (r *Reconciler) createPVAndPVCForModelVersion(modelVersion *modelv1alpha1.ModelVersion,
	pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) (err error) {

	// get pv name (different for local storage and remote storage)
	pvName := ""
	if modelVersion.Spec.Storage.LocalStorage != nil {
		if modelVersion.Spec.Storage.LocalStorage.NodeName != "" {
			pvName = setPVNameForLocalStorage(modelVersion.Name, modelVersion.Spec.Storage.LocalStorage.NodeName)
		} else {
			return fmt.Errorf("no node name set for local storage when setting pv, modeversion: %s", modelVersion.Name)
		}
	} else {
		pvName = setPVNameForNFS(modelVersion.Name)
	}

	// create pv if non-exist
	err = r.Get(context.Background(), types.NamespacedName{Name: pvName}, pv)
	if err != nil {
		if errors.IsNotFound(err) {
			storageProvider := registry.GetStorageProvider(modelVersion.Spec.Storage)
			pv = storageProvider.CreatePersistentVolume(modelVersion.Spec.Storage, pvName)
			if pv.OwnerReferences == nil {
				pv.OwnerReferences = make([]metav1.OwnerReference, 0)
			}
			// this pv is controlled by the modelversion which is going to use it
			pv.OwnerReferences = append(pv.OwnerReferences, metav1.OwnerReference{
				APIVersion: modelVersion.APIVersion,
				Kind:       modelVersion.Kind,
				Name:       modelVersion.Name,
				UID:        modelVersion.UID,
			})
			err = r.Create(context.Background(), pv)
			if err != nil {
				log.Info("failed to create pv for modelversion",
					"modelversion", modelVersion.Name, "pv", pv.Name)
				return err
			}
			log.Info("successfully create pv for modelversion",
				"modelversion", modelVersion.Name, "pv", pv.Name)
		} else {
			log.Error(err, fmt.Sprintf("modelversion %s failed to get pv", modelVersion.Name))
			return err
		}
	}

	// get pvc name (different for local storage and remote storage)
	pvcName := ""
	if modelVersion.Spec.Storage.LocalStorage != nil {
		if modelVersion.Spec.Storage.LocalStorage.NodeName != "" {
			pvcName = setPVCNameForLocalStorage(modelVersion.Name, modelVersion.Spec.Storage.LocalStorage.NodeName)
		} else {
			return fmt.Errorf("no node name set for local storage when setting pvc, modeversion: %s", modelVersion.Name)
		}
	} else {
		pvcName = setPVCNameForNFS(modelVersion.Name)
	}

	// create pvc if non-exist
	err = r.Get(context.Background(), types.NamespacedName{Namespace: modelVersion.Namespace, Name: pvcName}, pvc)
	if err != nil {
		if errors.IsNotFound(err) {
			// use the same class name to bound the corresponding pv
			className := ""
			pvc = &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: modelVersion.Namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					VolumeName:  pv.Name,
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("50Mi"),
						},
					},
					StorageClassName: &className,
				},
			}

			if pvc.OwnerReferences == nil {
				pvc.OwnerReferences = make([]metav1.OwnerReference, 0)
			}
			// this pvc is controlled by the modelversion which is going to use it
			pvc.OwnerReferences = append(pvc.OwnerReferences, metav1.OwnerReference{
				APIVersion: modelVersion.APIVersion,
				Kind:       modelVersion.Kind,
				Name:       modelVersion.Name,
				UID:        modelVersion.UID,
			})

			err = r.Create(context.Background(), pvc)
			if err != nil {
				log.Info("failed to create pvc for modelversion",
					"modelversion", modelVersion.Name, "pvc", pvc.Name)
				return err
			}
			log.Info("successfully create pvc for modelversion",
				"modelversion", modelVersion.Name, "pvc", pvc.Name)
		} else {
			log.Error(err, fmt.Sprintf("modelversion %s failed to get pvc", modelVersion.Name))
			return err
		}
	}

	return err
}

func setPVNameForNFS(modelName string) string {
	return "mv-pv-" + modelName
}

func setPVCNameForNFS(modelName string) string {
	return "mv-pvc-" + modelName
}

func setPVNameForLocalStorage(modelName string, nodeName string) string {
	return "mv-pv-" + modelName + "-" + nodeName
}

func setPVCNameForLocalStorage(modelName string, nodeName string) string {
	return "mv-pvc-" + modelName + "-" + nodeName
}

func getBuildImagePodName(modelName string, versionId string) string {
	return "image-build-" + modelName + "-" + versionId
}
