//go:build !ignore_autogenerated
// +build !ignore_autogenerated

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
// Code generated by deepcopy-gen. DO NOT EDIT.

package v1alpha1

import (
	modelv1alpha1 "github.com/hliangzhao/torch-on-k8s/apis/model/v1alpha1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DAGCondition) DeepCopyInto(out *DAGCondition) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DAGCondition.
func (in *DAGCondition) DeepCopy() *DAGCondition {
	if in == nil {
		return nil
	}
	out := new(DAGCondition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobCondition) DeepCopyInto(out *JobCondition) {
	*out = *in
	in.LastUpdateTime.DeepCopyInto(&out.LastUpdateTime)
	in.LastTransitionTime.DeepCopyInto(&out.LastTransitionTime)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobCondition.
func (in *JobCondition) DeepCopy() *JobCondition {
	if in == nil {
		return nil
	}
	out := new(JobCondition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *JobStatus) DeepCopyInto(out *JobStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]JobCondition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.TaskStatuses != nil {
		in, out := &in.TaskStatuses, &out.TaskStatuses
		*out = make(map[TaskType]*TaskStatus, len(*in))
		for key, val := range *in {
			var outVal *TaskStatus
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = new(TaskStatus)
				**out = **in
			}
			(*out)[key] = outVal
		}
	}
	if in.TorchElasticStatuses != nil {
		in, out := &in.TorchElasticStatuses, &out.TorchElasticStatuses
		*out = make(map[TaskType]*TorchElasticStatus, len(*in))
		for key, val := range *in {
			var outVal *TorchElasticStatus
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = new(TorchElasticStatus)
				(*in).DeepCopyInto(*out)
			}
			(*out)[key] = outVal
		}
	}
	if in.StartTime != nil {
		in, out := &in.StartTime, &out.StartTime
		*out = (*in).DeepCopy()
	}
	if in.CompletionTime != nil {
		in, out := &in.CompletionTime, &out.CompletionTime
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new JobStatus.
func (in *JobStatus) DeepCopy() *JobStatus {
	if in == nil {
		return nil
	}
	out := new(JobStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RunPolicy) DeepCopyInto(out *RunPolicy) {
	*out = *in
	if in.CleanPodPolicy != nil {
		in, out := &in.CleanPodPolicy, &out.CleanPodPolicy
		*out = new(CleanPodPolicy)
		**out = **in
	}
	if in.TTLSecondsAfterFinished != nil {
		in, out := &in.TTLSecondsAfterFinished, &out.TTLSecondsAfterFinished
		*out = new(int32)
		**out = **in
	}
	if in.ActiveDurations != nil {
		in, out := &in.ActiveDurations, &out.ActiveDurations
		*out = new(int64)
		**out = **in
	}
	if in.BackoffLimit != nil {
		in, out := &in.BackoffLimit, &out.BackoffLimit
		*out = new(int32)
		**out = **in
	}
	if in.SchedulingPolicy != nil {
		in, out := &in.SchedulingPolicy, &out.SchedulingPolicy
		*out = new(SchedulingPolicy)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RunPolicy.
func (in *RunPolicy) DeepCopy() *RunPolicy {
	if in == nil {
		return nil
	}
	out := new(RunPolicy)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SchedulingPolicy) DeepCopyInto(out *SchedulingPolicy) {
	*out = *in
	if in.MinAvailable != nil {
		in, out := &in.MinAvailable, &out.MinAvailable
		*out = new(int32)
		**out = **in
	}
	if in.Priority != nil {
		in, out := &in.Priority, &out.Priority
		*out = new(int32)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SchedulingPolicy.
func (in *SchedulingPolicy) DeepCopy() *SchedulingPolicy {
	if in == nil {
		return nil
	}
	out := new(SchedulingPolicy)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *SpotTaskSpec) DeepCopyInto(out *SpotTaskSpec) {
	*out = *in
	if in.Labels != nil {
		in, out := &in.Labels, &out.Labels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new SpotTaskSpec.
func (in *SpotTaskSpec) DeepCopy() *SpotTaskSpec {
	if in == nil {
		return nil
	}
	out := new(SpotTaskSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TaskSpec) DeepCopyInto(out *TaskSpec) {
	*out = *in
	if in.NumTasks != nil {
		in, out := &in.NumTasks, &out.NumTasks
		*out = new(int32)
		**out = **in
	}
	if in.SpotTaskSpec != nil {
		in, out := &in.SpotTaskSpec, &out.SpotTaskSpec
		*out = new(SpotTaskSpec)
		(*in).DeepCopyInto(*out)
	}
	in.Template.DeepCopyInto(&out.Template)
	if in.DependsOn != nil {
		in, out := &in.DependsOn, &out.DependsOn
		*out = make([]DAGCondition, len(*in))
		copy(*out, *in)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TaskSpec.
func (in *TaskSpec) DeepCopy() *TaskSpec {
	if in == nil {
		return nil
	}
	out := new(TaskSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TaskStatus) DeepCopyInto(out *TaskStatus) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TaskStatus.
func (in *TaskStatus) DeepCopy() *TaskStatus {
	if in == nil {
		return nil
	}
	out := new(TaskStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchElasticPolicy) DeepCopyInto(out *TorchElasticPolicy) {
	*out = *in
	if in.NumMinReplicas != nil {
		in, out := &in.NumMinReplicas, &out.NumMinReplicas
		*out = new(int32)
		**out = **in
	}
	if in.NumMaxReplicas != nil {
		in, out := &in.NumMaxReplicas, &out.NumMaxReplicas
		*out = new(int32)
		**out = **in
	}
	if in.NProcPerNode != nil {
		in, out := &in.NProcPerNode, &out.NProcPerNode
		*out = new(int32)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchElasticPolicy.
func (in *TorchElasticPolicy) DeepCopy() *TorchElasticPolicy {
	if in == nil {
		return nil
	}
	out := new(TorchElasticPolicy)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchElasticStatus) DeepCopyInto(out *TorchElasticStatus) {
	*out = *in
	if in.LastUpdateTime != nil {
		in, out := &in.LastUpdateTime, &out.LastUpdateTime
		*out = (*in).DeepCopy()
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchElasticStatus.
func (in *TorchElasticStatus) DeepCopy() *TorchElasticStatus {
	if in == nil {
		return nil
	}
	out := new(TorchElasticStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchJob) DeepCopyInto(out *TorchJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchJob.
func (in *TorchJob) DeepCopy() *TorchJob {
	if in == nil {
		return nil
	}
	out := new(TorchJob)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *TorchJob) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchJobList) DeepCopyInto(out *TorchJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]TorchJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchJobList.
func (in *TorchJobList) DeepCopy() *TorchJobList {
	if in == nil {
		return nil
	}
	out := new(TorchJobList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *TorchJobList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchJobSpec) DeepCopyInto(out *TorchJobSpec) {
	*out = *in
	in.RunPolicy.DeepCopyInto(&out.RunPolicy)
	if in.TorchTaskSpecs != nil {
		in, out := &in.TorchTaskSpecs, &out.TorchTaskSpecs
		*out = make(map[TaskType]*TaskSpec, len(*in))
		for key, val := range *in {
			var outVal *TaskSpec
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = new(TaskSpec)
				(*in).DeepCopyInto(*out)
			}
			(*out)[key] = outVal
		}
	}
	if in.MinMembers != nil {
		in, out := &in.MinMembers, &out.MinMembers
		*out = make(map[TaskType]*int32, len(*in))
		for key, val := range *in {
			var outVal *int32
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = new(int32)
				**out = **in
			}
			(*out)[key] = outVal
		}
	}
	if in.ModelVersion != nil {
		in, out := &in.ModelVersion, &out.ModelVersion
		*out = new(modelv1alpha1.ModelVersion)
		(*in).DeepCopyInto(*out)
	}
	if in.TorchElasticPolicy != nil {
		in, out := &in.TorchElasticPolicy, &out.TorchElasticPolicy
		*out = new(TorchElasticPolicy)
		(*in).DeepCopyInto(*out)
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchJobSpec.
func (in *TorchJobSpec) DeepCopy() *TorchJobSpec {
	if in == nil {
		return nil
	}
	out := new(TorchJobSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TorchJobStatus) DeepCopyInto(out *TorchJobStatus) {
	*out = *in
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TorchJobStatus.
func (in *TorchJobStatus) DeepCopy() *TorchJobStatus {
	if in == nil {
		return nil
	}
	out := new(TorchJobStatus)
	in.DeepCopyInto(out)
	return out
}
