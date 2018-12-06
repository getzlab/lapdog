#!/usr/bin/python

# Copyright 2017 Google Inc.
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

# cromwell_driver.py
#
# This script provides a library interface to Cromwell, namely:
#    * Start the Cromwell server
#    * Submit execution requests to Cromwell
#    * Poll Cromwell for job status

import logging
import os
import subprocess
import time
import json
import csv

import requests

import sys_util
import sys
import atexit
import itertools

def clump(seq, length):
    getter = iter(seq)
    while True:
        try:
            tmp = next(getter)
            yield itertools.chain([tmp], itertools.islice(getter, length-1))
        except StopIteration:
            return


class CromwellDriver(object):

    def __init__(self, cromwell_conf, cromwell_jar):
        self.cromwell_conf = cromwell_conf
        self.cromwell_jar = cromwell_jar

        self.cromwell_proc = None

        self.batch_submission = False

    def start(self, memory=3):
        """Start the Cromwell service."""
        if self.cromwell_proc:
            logging.info("Request to start Cromwell: already running")
            return

        self.cromwell_proc = subprocess.Popen([
                'java',
                '-Dconfig.file=' + self.cromwell_conf,
                '-Xmx%dg'%memory,
                '-jar', self.cromwell_jar,
                'server'])

        logging.info("Started Cromwell")

    def fetch(self, wf_id=None, post=False, files=None, method=None):
        url = 'http://localhost:8000/api/workflows/v1'
        if wf_id is not None:
            url = os.path.join(url, wf_id)
        if method is not None:
            url = os.path.join(url, method)
        if post:
            r = requests.post(url, files=files)
        else:
            r = requests.get(url)
        return r.json()

    def abort(self, workflow_id):
        return requests.post(
            'http://localhost:8000/api/workflows/v1/{0}/abort'.format(workflow_id)
        ).json()

    def batch(self, submission_id, wdl, inputs, options, batch_limit, query_limit):
        logging.info("Starting batch request. Waiting for cromwell to start...")
        time.sleep(60)
        with open(wdl, 'rb') as wdlReader:
            with open(options, 'rb') as optionReader:
                data = {
                    'workflowSource': wdlReader.read(),
                    # 'workflowInputs': json.dumps([line for line in reader]),
                    'workflowOptions': optionReader.read(),
                    'labels': json.dumps({
                        'lapdog-submission-id':submission_id,
                        'lapdog-execution-role':'worker'
                    }).encode()
                }
        logging.info("Starting the following configuration: " + json.dumps(data))
        output = []
        with open(inputs, 'rb') as inputReader:
            reader = csv.DictReader(inputReader, delimiter='\t', lineterminator='\n')
            for batch in clump(reader, batch_limit):
                logging.info("Running a new batch of %d workflows" % batch_limit)

                chunk = []

                for group in clump(batch, query_limit):
                    logging.info("Starting a chunk of %d workflows" % query_limit)
                    response = None
                    for attempt in range(10):
                        try:
                            data['workflowInputs'] = json.dumps([line for line in group])
                            response = requests.post(
                                'http://localhost:8000/api/workflows/v1/batch',
                                files=data
                            ).json()
                            logging.info("Submitted jobs. Begin polling")
                            break
                        except requests.exceptions.ConnectionError as e:
                            logging.info("Failed to connect to Cromwell (attempt %d): %s",
                                attempt + 1, e)
                            time.sleep(30)

                    if not response:
                        sys_util.exit_with_error(
                                "Failed to connect to Cromwell after {0} seconds".format(
                                        300))

                    logging.info("Raw response: " + repr(response))

                    for job in response:
                        if job['status'] != 'Submitted':
                            for job in response:
                                self.abort(job['id'])
                            sys_util.exit_with_error(
                                    "Job {} status from Cromwell was not 'Submitted', instead '{}'".format(
                                            job['id'], job['status']))
                        else:
                            chunk.append(job)

                self.batch_submission = True

                @atexit.register
                def abort_all_jobs():
                    if self.batch_submission:
                        for job in response:
                            self.abort(job['id'])

                for i in range(12):
                    time.sleep(5)

                attempt = 0
                max_failed_attempts = 3
                known_failures = set()
                while True:
                    for i in range(3):
                        time.sleep(5)

                    # Cromwell occassionally fails to respond to the status request.
                    # Only give up after 3 consecutive failed requests.
                    try:
                        status_json = [
                            self.fetch(wf_id=job['id'], method='status')
                            for job in chunk
                        ]
                        attempt = 0
                    except requests.exceptions.ConnectionError as e:
                        attempt += 1
                        logging.info("Error polling Cromwell job status (attempt %d): %s",
                            attempt, e)

                        if attempt >= max_failed_attempts:
                            sys_util.exit_with_error(
                                "Cromwell did not respond for %d consecutive requests" % attempt)

                        continue

                    statuses = {job['status'] for job in status_json}
                    # logging.info("<WORKFLOW STATUS UPDATE> %s" % json.dumps(status_json))
                    if 'Failed' in statuses:
                        new_failures = [
                            job for job in status_json
                            if job['status'] == 'Failed' and job['id'] not in known_failures
                        ]
                        if len(new_failures):
                            sys.stderr.write(
                                 "The following jobs failed: %s\n" % (
                                     ', '.join('%s (%s)' % (job['id'], job['status']) for job in new_failures)
                                 )
                             )
                        known_failures |= {job['id'] for job in new_failures}
                    if not len(statuses - {'Succeeded', 'Failed', 'Aborted'}):
                        logging.info("All workflows in terminal states")
                        break

                self.batch_submission = False
                logging.info("<SUBMISSION COMPLETE. FINALIZING DATA>")

                output += [
                    {
                        'workflow_id':job['id'],
                        'workflow_status':job['status'],
                        'workflow_output': self.fetch(wf_id=job['id'], method='outputs') if job['status'] == 'Succeeded' else None,
                        'workflow_metadata': self.fetch(wf_id=job['id'], method='metadata') if job['status'] == 'Succeeded' else None,
                    }
                    for job in status_json
                ]
        return output



    def submit(self, wdl, workflow_inputs, workflow_options, sleep_time=15):
        """Post new job to the server and poll for completion."""

        # Add required input files
        with open(wdl, 'rb') as f:
            wdl_source = f.read()
        with open(workflow_inputs, 'rb') as f:
            wf_inputs = f.read()

        files = {
                'wdlSource': wdl_source,
                'workflowInputs': wf_inputs,
        }

        # Add workflow options if specified
        if workflow_options:
            with open(workflow_options, 'rb') as f:
                wf_options = f.read()
                files['workflowOptions'] = wf_options

        # After Cromwell start, it may take a few seconds to be ready for requests.
        # Poll up to a minute for successful connect and submit.

        job = None
        max_time_wait = 60
        wait_interval = 5

        time.sleep(wait_interval)
        for attempt in range(max_time_wait/wait_interval):
            try:
                job = self.fetch(post=True, files=files)
                break
            except requests.exceptions.ConnectionError as e:
                logging.info("Failed to connect to Cromwell (attempt %d): %s",
                    attempt + 1, e)
                time.sleep(wait_interval)

        if not job:
            sys_util.exit_with_error(
                    "Failed to connect to Cromwell after {0} seconds".format(
                            max_time_wait))

        if job['status'] != 'Submitted':
            sys_util.exit_with_error(
                    "Job status from Cromwell was not 'Submitted', instead '{0}'".format(
                            job['status']))

        # Job is running.
        cromwell_id = job['id']
        logging.info("Job submitted to Cromwell. job id: %s", cromwell_id)

        # Poll Cromwell for job completion.
        attempt = 0
        max_failed_attempts = 3
        while True:
            time.sleep(sleep_time)

            # Cromwell occassionally fails to respond to the status request.
            # Only give up after 3 consecutive failed requests.
            try:
                status_json = self.fetch(wf_id=cromwell_id, method='status')
                attempt = 0
            except requests.exceptions.ConnectionError as e:
                attempt += 1
                logging.info("Error polling Cromwell job status (attempt %d): %s",
                    attempt, e)

                if attempt >= max_failed_attempts:
                    sys_util.exit_with_error(
                        "Cromwell did not respond for %d consecutive requests" % attempt)

                continue

            status = status_json['status']
            if status == 'Succeeded':
                break
            elif status == 'Submitted':
                pass
            elif status == 'Running':
                pass
            else:
                sys_util.exit_with_error(
                        "Status of job is not Submitted, Running, or Succeeded: %s" % status)

        logging.info("Cromwell job status: %s", status)

        # Cromwell produces a list of outputs and full job details
        outputs = self.fetch(wf_id=cromwell_id, method='outputs')
        metadata = self.fetch(wf_id=cromwell_id, method='metadata')

        return outputs, metadata


if __name__ == '__main__':
    pass
