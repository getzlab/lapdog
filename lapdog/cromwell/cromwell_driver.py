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
from simplejson import JSONDecodeError
import csv

import requests

import sys_util
import sys
import atexit
import traceback
import itertools
import shlex

def clump(seq, length):
    getter = iter(seq)
    while True:
        try:
            tmp = next(getter)
            yield itertools.chain([tmp], itertools.islice(getter, length-1))
        except StopIteration:
            return

# https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

def unpack(data):
    for k,v in data.items():
        if isinstance(v, str):
            if v.startswith('[') and v.endswith(']'):
                try:
                    data[k] = json.loads(v.replace("\\'", '~<BACKQUOTE>').replace("'", '"').replace('~<BACKQUOTE>', "\\'"))
                except:
                    pass
            elif v.startswith('{') and v.endswith('}'):
                try:
                    data[k] = json.loads(v.replace("\\'", '~<BACKQUOTE>').replace("'", '"').replace('~<BACKQUOTE>', "\\'"))
                except:
                    pass
    if len(json.dumps(data)) >= 10485760:
        raise ValueError("The size of input metadata for an individual workflow cannot exceed 10 Mib")
    return data

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

        self.mem = memory

        logging.info("Started Cromwell")

    def check_cromwell(self):
        status = self.cromwell_proc.poll()
        if status == -9:
            logging.error("Cromwell Proc was killed. Retry with additional memory")
        elif status is not None:
            logging.error("Cromwell Proc exit with status: %d" % status)

    def fetch(self, session, wf_id=None, post=False, files=None, method=None):
        url = 'http://localhost:8000/api/workflows/v1'
        if wf_id is not None:
            url = os.path.join(url, wf_id)
        if method is not None:
            url = os.path.join(url, method)
        if post:
            r = session.post(url, files=files)
        else:
            for attempt in range(3):
                try:
                    r = session.get(url)
                    assert r.status_code < 400
                    break
                except requests.ConnectionError:
                    logging.info("Request failed: Connection Error")
                    if attempt == 2:
                        raise
                    time.sleep(10)
                except AssertionError:
                    logging.info("Request failed (%d) : %s" % (r.status_code, r.text))
                    if attempt == 2:
                        raise
                    time.sleep(10)
                    r.connection.close()
        r.connection.close()
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
        first = True
        with open(inputs, 'rb') as inputReader:
            reader = csv.DictReader(inputReader, delimiter='\t', lineterminator='\n')
            with requests.Session() as session:
                for batch in clump(reader, batch_limit):
                    self.check_cromwell()
                    logging.info("Running a new batch of %d workflows" % batch_limit)

                    chunk = []

                    if not first:
                        logging.info("Restarting cromwell...")
                        self.cromwell_proc.kill()
                        self.cromwell_proc = None
                        time.sleep(10)
                        self.start(self.mem)
                        time.sleep(20)
                        logging.info("Resuming next batch")
                    else:
                        first = False

                    for group in clump(batch, query_limit):
                        logging.info("Starting a chunk of %d workflows" % query_limit)
                        group = [line for line in group]
                        logging.info("There are %d workflows in this group" % len(group))
                        response = None
                        for attempt in range(10):
                            try:
                                data['workflowInputs'] = json.dumps([unpack(line) for line in group])
                                response = session.post(
                                    'http://localhost:8000/api/workflows/v1/batch',
                                    files=data
                                )
                                response = response.json()
                                logging.info("Submitted jobs. Begin polling")
                                break
                            except requests.exceptions.ConnectionError as e:
                                traceback.print_exc()
                                self.check_cromwell()
                                logging.info("Failed to connect to Cromwell (attempt %d): %s",
                                    attempt + 1, e)
                                time.sleep(30)
                            except JSONDecodeError:
                                traceback.print_exc()
                                self.check_cromwell()
                                logging.error("Unexpected response from Cromwell: (%d) : %s" % (response.status_code, response.text))
                                raise

                        if not response:
                            self.check_cromwell()
                            sys_util.exit_with_error(
                                    "Failed to connect to Cromwell after {0} seconds".format(
                                            300))

                        logging.info("Raw response: " + repr(response))

                        for job in response:
                            if job['status'] != 'Submitted' and job['status'] != 'Running':
                                for job in response:
                                    self.abort(job['id'])
                                sys_util.exit_with_error(
                                        "Job {} status from Cromwell was not 'Submitted', instead '{}'".format(
                                                job['id'], job['status']))
                            else:
                                chunk.append(job)

                    self.batch_submission = True
                    self.check_cromwell()

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
                            time.sleep(10)

                        self.check_cromwell()

                        # Cromwell occassionally fails to respond to the status request.
                        # Only give up after 3 consecutive failed requests.
                        try:
                            status_json = [
                                [
                                    self.fetch(session, wf_id=job['id'], method='status'),
                                    time.sleep(0.1)
                                ][0]
                                for job in chunk
                            ]
                            attempt = 0
                        except requests.exceptions.ConnectionError as e:
                            attempt += 1
                            logging.info("Error polling Cromwell job status (attempt %d): %s",
                                attempt, e)
                            self.check_cromwell()

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
                            'workflow_output': self.fetch(session, wf_id=job['id'], method='outputs') if job['status'] == 'Succeeded' else None,
                            'workflow_metadata': self.fetch(session, wf_id=job['id'], method='metadata') if job['status'] == 'Succeeded' else None,
                        }
                        for job in status_json
                    ]

                    self.check_cromwell()

                    if 'Aborted' in statuses:
                        # Quit now. No reason to start a new batch to get aborted
                        sys.stderr.write("There were aborted workflows. Aborting submission now.")
                        return output
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
                job = self.fetch(session, post=True, files=files)
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
                status_json = self.fetch(session, wf_id=cromwell_id, method='status')
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
        outputs = self.fetch(session, wf_id=cromwell_id, method='outputs')
        metadata = self.fetch(session, wf_id=cromwell_id, method='metadata')

        return outputs, metadata


if __name__ == '__main__':
    pass
