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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	coordinatorQueuePendingJobsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "torch_on_k8s_tenant_queue_jobs_pending_count",
		Help: "Counter for accounting pending jobs in each tenant queue",
	}, []string{"queue"})
)
