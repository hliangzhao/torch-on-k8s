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

package plugins

import (
	pkgcoordinator "github.com/hliangzhao/torch-on-k8s/pkg/coordinator"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const (
	DefaultSchedulingPeriod = 100 * time.Millisecond
)

type (
	PluginFactory func(client client.Client, recorder record.EventRecorder) pkgcoordinator.Plugin
	Registry      map[string]PluginFactory
)

func NewPluginRegistry() Registry {
	return map[string]PluginFactory{
		PriorityPluginName: NewPriorityPlugin,
		QuotaPluginName:    NewQuotaPlugin,
	}
}

func NewCoordinateConfiguration() pkgcoordinator.CoordinateConfiguration {
	return pkgcoordinator.CoordinateConfiguration{
		SchedulingPeriod:  DefaultSchedulingPeriod,
		TenantPlugin:      QuotaPluginName,
		FilterPlugins:     []string{QuotaPluginName},
		ScorePlugins:      []string{PriorityPluginName},
		PreDequeuePlugins: []string{QuotaPluginName},
	}
}
