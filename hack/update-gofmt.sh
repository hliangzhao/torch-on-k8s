set -eou pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_ROOT}/.." && pwd)"

echo "run gofmt in directory $REPO_ROOT now"
find "$REPO_ROOT" -name "*.go" | grep -v -e "/zz_generated.*.go" -e "/*kubebuilder.go" -e "/doc.go" | xargs gofmt -s -w
echo "done"
