#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

echo "For install generated codes at right position, the project root should be GOPATH/src."
echo "Or you need to manually copy the generated code to the right place."

SCRIPT_ROOT=$(unset CDPATH && cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)

# Generate for resources defined in pkg/common
echo "Generate for pkg/common..."
bash "${SCRIPT_ROOT}"/hack/generate-internal-groups.sh "deepcopy,defaulter" \
  github.com/hliangzhao/torch-on-k8s/pkg/common github.com/hliangzhao/torch-on-k8s/pkg/common github.com/hliangzhao/torch-on-k8s/pkg/common \
  "apis:v1alpha1" \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt
printf "\n"

# Generate for resources defined in apis/train
echo "Generate for apis..."
bash "${SCRIPT_ROOT}"/hack/generate-groups.sh "deepcopy,client,informer,lister" \
  github.com/hliangzhao/torch-on-k8s/client github.com/hliangzhao/torch-on-k8s/apis \
  "train:v1alpha1" \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt
bash "${SCRIPT_ROOT}"/hack/generate-internal-groups.sh "defaulter" \
  github.com/hliangzhao/torch-on-k8s/client github.com/hliangzhao/torch-on-k8s/apis github.com/hliangzhao/torch-on-k8s/apis \
  "train:v1alpha1" \
  --go-header-file "${SCRIPT_ROOT}"/hack/boilerplate.go.txt

echo "done"
