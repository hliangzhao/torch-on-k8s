## torch-on-k8s

This is a minimal implementation for running distributed torch training jobs in k8s cluster (7k lines of code). 
In this implementation, we introduce a CRD called torchjob, which is composed of multiple tasks (each task has a type, 
for example, master or worker), and each task is a wrapper of a pod.

Supported features:
* DAG scheduling and Gang scheduling is enabled with [Volcano](https://github.com/volcano-sh/volcano).
* Native job scheduling and coordination (a torchjob will be enqueued into a queue, each time the coordinator 
  selects a queue and a torchjob in the queue to do the reconciliation).
* Elastic scaling of torchjob task replicas.

Something new compared with [kubedl](https://github.com/kubedl-io/kubedl):

* A [greedy heuristic](https://github.com/hliangzhao/torch-on-k8s/blob/cb0dd4d1dd5afa830426e97112d16ba1de49f4e9/controllers/train/torchjob_controller.go#L230) for node selection to maximize the storage utilization and performance for the saving of 
  output models (designed for local storage).
* A [weighted-round-robin (WRR)](https://github.com/hliangzhao/torch-on-k8s/blob/cb0dd4d1dd5afa830426e97112d16ba1de49f4e9/pkg/coordinator/core/policy.go#L89) algorithm implementation for queue selections in job coordination.
* Global optimization:
  + Use in-memory cache to improve performance (remove repeating label & annotation generations, etc.)
  + Create service & pod label selectors outside loops to avoid repeating creation.
* Fix some hiding bugs:
  + In pkg/common/failover.go, `restartPod()` should return `false` rather than `err == nil` when the crr resource 
    status is `kruisev1alpha1.ContainerRecreateRequestFailed`. Otherwise, there is a risk that the to-be-inplace-restart 
    pod will be marked as succeeded when it is not.

### TODO

* For queue selection, implement smooth-weighted-round-robin algorithm.
* Native elastic training implementation with [TorchElastic](https://pytorch.org/docs/stable/elastic/agent.html).
* Add tests and examples.

### References

* https://github.com/kubedl-io/kubedl
* https://github.com/kubeflow/kubeflow
* https://github.com/volcano-sh/volcano
