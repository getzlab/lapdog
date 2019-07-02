#!/bin/bash

# Copyright 2017 Google Inc.
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

set -o errexit
set -o nounset
set -eo pipefail

touch stdout.log stderr.log pipeline-stderr.log

function cleanup {
  gsutil -h "Content-Type:text/plain" cp stdout.log stderr.log pipeline-stderr.log $LAPDOG_LOG_PATH
  gsutil rm "${WDL}"
}

trap cleanup EXIT

readonly INPUT_PATH=/pipeline/input

# WDL, INPUTS, and OPTIONS file contents are all passed into
# the pipeline as environment variables - write them out as
# files.
mkdir -p "${INPUT_PATH}"
gsutil cp "${WDL}" "${INPUT_PATH}/wf.wdl"
gsutil cp "${WORKFLOW_INPUTS}" "${INPUT_PATH}/wf.inputs.json"
echo "${WORKFLOW_OPTIONS}" > "${INPUT_PATH}/wf.options.json"

# Set the working directory to the location of the scripts
readonly SCRIPT_DIR=$(dirname $0)
cd "${SCRIPT_DIR}"

if [ "${LAPDOG_SUBMISSION_ID}" ]
then
  export BATCH_ARG="--batch ${LAPDOG_SUBMISSION_ID}"
fi

# Update jes template
sed s/SERVICEACCOUNT/$(gcloud config get-value account)/ < /cromwell/jes_template.tmp.conf | \
 sed s/PRIVATE_ACCESS/${PRIVATE_ACCESS}/ | \
 sed s/SUBMISSION_ZONES/"${SUBMISSION_ZONES}"/ > /cromwell/jes_template.conf

cat /cromwell/jes_template.conf

# Boot SQL and prefil call cache
chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
service mysql start

mysql -uroot -pcromwell <<< "CREATE DATABASE cromwell;"

export METAGENERATION=$(gsutil stat $DUMP_PATH || echo "")

if [[ -n "$METAGENERATION" ]]
then
  gsutil cat $DUMP_PATH | mysql -uroot -pcromwell cromwell
fi

# Execute the wdl_runner
python -u wdl_runner.py \
 --wdl "${INPUT_PATH}"/wf.wdl \
 --workflow-inputs "${INPUT_PATH}"/wf.inputs.json \
 --working-dir "${WORKSPACE}" \
 --workflow-options "${INPUT_PATH}"/wf.options.json \
 --output-dir "${OUTPUTS}" \
 $BATCH_ARG 2> stderr.log | python logger.py $LAPDOG_LOG_PATH/pipeline-stdout.log > stdout.log 2> pipeline-stderr.log

if [[ -n "$DUMP_PATH" ]]
then
  if [[ "$METAGENERATION" != $(gsutil stat $DUMP_PATH) ]]
  then
    echo "Warning: Cromwell Call Cache has changed since this submission started. Changes will be overwritten by this submission" >> stderr.log
  fi

  gsutil cp <(mysqldump -uroot -pcromwell cromwell) $DUMP_PATH
fi
