#!/bin/bash

set -o errexit
set -o nounset
set -eo pipefail

touch stdout.log stderr.log

function cleanup {
  gsutil -h "Content-Type:text/plain" cp stdout.log stderr.log $LAPDOG_LOG_PATH
}

trap cleanup EXIT

(
  git clone $LAPDOG_CLONE_URL && \
    cd lapdog && \
    git checkout $LAPDOG_TAG && \
    python3 -m pip install -e . && \
    lapdog apply-patch $LAPDOG_NAMESPACE
) > stdout.log 2> stderr.log
