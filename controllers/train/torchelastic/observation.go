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
	"bufio"
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog"
	"regexp"
	"strconv"
	"strings"
)

func podRunning(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning
}

func GetDefaultWorkerName(jobName string) string {
	return jobName + "-" + "worker-0"
}

func getMetricsObservation(client *kubernetes.Clientset, namespace, name string) (MetricObservation, error) {
	lines := int64(1)
	logOptions := &corev1.PodLogOptions{
		TailLines: &lines,
		Follow:    true,
	}

	// getMetricsObservation current line from the log
	curLine, err := readOneLineOfLog(client, namespace, name, logOptions)
	if err != nil {
		return MetricObservation{}, err
	}

	// parse the segments of the log
	rawLog := strings.Split(curLine, "\t")
	epochRule := regexp.MustCompile(`[0-9]{1,2}`)
	batchRule := regexp.MustCompile(`[0-9]{2,4}`)
	trainRule := regexp.MustCompile(`[0-9]{1,2}.[0-9]{3}`)
	accRule := regexp.MustCompile(`[0-9]{1,2}.[0-9]{1,2}`)

	// parse is required only when this line of log is a torchelastic training log
	match, err := regexp.MatchString(`Epoch`, rawLog[0])
	if err != nil {
		return MetricObservation{}, err
	}
	if match {
		numEpoch, _ := strconv.Atoi(epochRule.FindStringSubmatch(rawLog[0])[0])
		numBatch, _ := strconv.Atoi(batchRule.FindStringSubmatch(rawLog[0])[0])
		epochTrainTime, _ := strconv.ParseFloat(trainRule.FindStringSubmatch(rawLog[1])[0], 64)
		accuracy, _ := strconv.ParseFloat(accRule.FindStringSubmatch(rawLog[5])[0], 64)

		observe := MetricObservation{
			Accuracy: accuracy,
			Epoch:    int32(numEpoch),
			Latency:  epochTrainTime,
			Batch:    int32(numBatch),
		}
		if epochTrainTime > 1 {
			return MetricObservation{}, fmt.Errorf("epoch training time > 1, drop it")
		}
		klog.Infof("epoch: %d, batch: %d, training time: %f, accuracy: %f",
			numEpoch, numBatch, epochTrainTime, accuracy)
		return observe, nil
	}
	return MetricObservation{}, fmt.Errorf("current line of log is not a torchelastic training log")
}

// readOneLineOfLog reads one line of log from the given pod.
func readOneLineOfLog(client kubernetes.Interface, namespace, podName string, logOptions *corev1.PodLogOptions) (string, error) {
	readCloser, err := client.CoreV1().RESTClient().Get().Namespace(namespace).Name(podName).
		Resource("pods").SubResource("log").
		VersionedParams(logOptions, scheme.ParameterCodec).Stream(context.TODO())
	if err != nil {
		return "", err
	}

	defer func() {
		_ = readCloser.Close()
	}()

	reader := bufio.NewReader(readCloser)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}
