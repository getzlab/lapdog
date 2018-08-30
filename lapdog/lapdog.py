import os
import json
import dalmatian as dog
from firecloud import api as fc
import contextlib
import csv
from google.cloud import storage
from agutil.parallel import parallelize, parallelize2
from agutil import status_bar, byteSize
import sys
import re
import tempfile
import time
import subprocess
from hashlib import md5
import base64
import yaml
from glob import glob
from io import StringIO
from . import adapters
from .adapters import getblob, get_operation_status, mtypes
from .cache import cache_init
from .operations import APIException, Operator, capture
from itertools import repeat
import pandas as pd

lapdog_id_pattern = re.compile(r'[0-9a-f]{32}')
global_id_pattern = re.compile(r'lapdog/(.+)')
lapdog_submission_pattern = re.compile(r'.*?/?lapdog-executions/([0-9a-f]{32})/submission.json')
creation_success_pattern = re.compile(r'Workspace (.+)/(.+) successfully')

timestamp_format = '%Y-%m-%dT%H:%M:%S.000%Z'

@contextlib.contextmanager
def dalmatian_api():
    try:
        yield
    except AssertionError as e:
        raise APIException("The Firecloud API has returned an unknown failure condition") from e

@contextlib.contextmanager
def open_if_string(obj, mode, *args, **kwargs):
    if isinstance(obj, str):
        with open(obj, mode, *args, **kwargs) as f:
            yield f
    else:
        yield obj

@contextlib.contextmanager
def dump_if_file(obj):
    if isinstance(obj, str):
        yield obj
    else:
        data = obj.read()
        mode = 'w' + ('b' if isinstance(data, Bytes) else '')
        with tempfile.NamedTemporaryFile(mode) as tmp:
            tmp.write(data)
            tmp.flush()
            yield tmp.name

def check_api(result):
    if result.status_code >= 400:
        raise APIException("The Firecloud API has returned status %d : %s" % (result.status_code, result.text))
    return result

def build_input_key(template):
    data = ''
    for k in sorted(template):
        if template[k] is not None:
            data += str(template[k])
    return md5(data.encode()).hexdigest()

@parallelize2()
def upload(bucket, path, source):
    blob = bucket.blob(path)
    # print("Commencing upload:", source)
    blob.upload_from_filename(source)

def purge_cache():
    for path in glob(cache_init()+'/*'):
        os.remove(path)


class BucketUploader(object):
    def __init__(self, bucket, prefix, key):
        self.bucket = bucket
        self.prefix = prefix
        self.key = key

    def upload(self, path, parent):
        bucket_path = os.path.join(
            self.prefix,
            parent[self.key] if self.key is not None else parent,
            os.path.basename(path)
        )
        return (
            'gs://%s/%s' % (self.bucket.id, bucket_path),
            upload(self.bucket, bucket_path, path)
        )

    def upload_df(self, df):

        uploads = []

        def scan_row(row):
            for i, value in enumerate(row):
                if os.path.isfile(value):
                    path, callback = self.upload(value, row.name)
                    row.iloc[i] = path
                    uploads.append(callback)
            return row

        return df.apply(scan_row, axis='columns'), uploads

def complete_execution(submission_id):
    """
    Checks a GCP job status and returns results to firecloud, if possible
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager(ns, ws).complete_execution(sid)
    raise TypeError("Global complete_execution can only operate on lapdog global ids")

def get_submission(submission_id):
    """
    Gets submission metadata from a lapdog or firecloud submission
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager(ns, ws).get_submission(sid)
    raise TypeError("Global get_submission can only operate on lapdog global ids")


class WorkspaceManager(dog.WorkspaceManager):
    def __init__(self, reference, workspace=None, timezone='America/New_York'):
        """
        Various argument configurations:
        * Dalmatian-style: Workspace('namespace', 'workspace')
        * Lapdog-style: Workspace('namespace/workspace')
        Returns a Workspace object
        """
        if workspace is not None:
            namespace = reference
        elif '/' in reference:
            namespace, workspace = reference.split('/')
        else:
            raise ValueError("Invalid argument combination")
        super().__init__(
            namespace,
            workspace,
            timezone
        )
        self.operator = Operator(self)

    @property
    def pending_operations(self):
        return len(self.operator.pending)

    @property
    def live(self):
        return self.operator.live

    def sync(self):
        is_live, exceptions = self.operator.go_live()
        if len(exceptions):
            print("There were", len(exceptions), "exceptions while attempting to sync with firecloud")
        return is_live, exceptions

    def get_bucket_id(self):
        with dalmatian_api():
            return super().get_bucket_id()

    def create_workspace(self, parent=None):
        with capture() as (stdout, stderr):
            with dalmatian_api():
                super().create_workspace(parent)
            stdout.seek(0,0)
            stderr.seek(0,0)
            text = stdout.read() + stderr.read()
        return bool(creation_success_pattern.search(text))

    def upload_entities(self, etype, df, index=True):
        #FIXME: This includes entity rewrites
        self.operator.update_entities_df(etype, df)

    def get_samples(self):
        return self.operator.get_entities_df('sample')

    def get_participants(self):
        return self.operator.get_entities_df('participant')

    def get_pairs(self):
        return self.operator.get_entities_df('pair')

    def get_sample_sets(self):
        return self.operator.get_entities_df('sample_set')

    def get_participant_sets(self):
        return self.operator.get_entities_df('participant_set')

    def get_pair_sets(self):
        return self.operator.get_entities_df('pair_set')

    def prepare_entity_df(self, etype, df):
        """
        Takes a dataframe of entity attributes.
        Uploads attributes which reference valid filepaths
        Returns a dataframe suitiable to upload
        """
        df, uploads = BucketUploader(
            storage.Client().get_bucket(self.get_bucket_id()),
            etype+'s',
            None
        ).upload_df(df)
        if len(uploads):
            _ = [callback() for callback in status_bar.iter(uploads)]
        return df

    def prepare_sample_df(self, df):
        """
        Takes a dataframe of sample attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('sample', df)

    def prepare_pair_df(self, df):
        """
        Takes a dataframe of pair attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('pair', df)

    def prepare_participant_df(self, df):
        """
        Takes a dataframe of sample attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('participant', df)

    def prepare_sample_set_df(self, df):
        """
        Takes a dataframe of sample_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('sample_set', df)

    def prepare_pair_set_df(self, df):
        """
        Takes a dataframe of pair_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('pair_set', df)

    def prepare_participant_set_df(self, df):
        """
        Takes a dataframe of participant_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('participant_set', df)

    def upload_entities(self, etype, df, index=True):
        return self.operator.update_entities_df(etype, df, index)

    def update_configuration(self, config, wdl=None, name=None, namespace=None):
        """
        Update a method configuration and (optionally) the WDL
        Must provide a properly formed configuration object.
        If a wdl is provided, it must be a filepath or file-like object
        If a wdl is provided, the method name is specified by the name parameter
        or methodRepoMethod.methodName of the config, if name is None
        If a wdl is provided, the method namespace is specified by the namespace parameter
        or methodRepoMethod.methodNamespace of the config, if the namespace is None
        If methodRepoMethod.methodVersion is set to 'latest' in the config, it will
        be set to the most recent version of the method
        """
        if namespace is not None:
            config['methodRepoMethod']['methodNamespace'] = namespace
        if name is not None:
            config['methodRepoMethod']['methodName'] = name
        version = None
        if wdl is not None:
            with dump_if_file(wdl) as wdl_path:
                with capture() as (stdout, stderr):
                    self.operator.upload_wdl(
                        config['methodRepoMethod']['methodNamespace'],
                        config['methodRepoMethod']['methodName'],
                        "Runs " + config['methodRepoMethod']['methodName'],
                        wdl_path
                    )
                    stdout.seek(0,0)
                    out_text = stdout.read()
            result = re.search(r'New SnapshotID: (\d+)', out_text)
            if result:
                version = int(result.group(1))
        if config['methodRepoMethod']['methodVersion'] == 'latest':
            if version is not None:
                with dalmatian_api():
                    version = int(dog.get_method_version(
                        config['methodRepoMethod']['methodNamespace'],
                        config['methodRepoMethod']['methodName']
                    ))
            config['methodRepoMethod']['methodVersion'] = version
        return self.operator.add_config(config)

    def update_attributes(self, attr_dict=None, **attrs):
        """
        Updates workspace attributes using the keyword arguments to this function
        Any values which reference valid filepaths will be uploaded to the workspace
        """
        if attr_dict is None:
            attr_dict = {}
        attr_dict.update(attrs)
        uploader = BucketUploader(
            storage.Client().get_bucket(self.get_bucket_id()),
            'workspace',
            None
        )
        uploads = []
        for k in attrs:
            if isinstance(attrs[k], str) and os.path.isfile(attrs[k]):
                path, callback = uploader.upload(
                    attrs[k],
                    os.path.basename(attrs[k])
                )
                attrs[k] = path
                uploads.append(callback)
        if len(uploads):
            _ = [callback() for callback in status_bar.iter(uploads)]
        self.operator.update_attributes(attrs)
        return attrs

    def get_attributes(self):
        return self.operator.attributes

    def create_submission(self, config_name, entity, expression=None, etype=None, use_cache=True):
        """
        Validates config parameters then creates a submission in Firecloud
        """
        if not self.live:
            print("The workspace is currently in offline mode. Please call WorkspaceManager.sync() to reconnect to firecloud", file=sys.stderr)
            return
        with dalmatian_api():
            configs = {
                cfg['name']:cfg for cfg in self.list_configs()
            }
        if config_name not in configs:
            raise KeyError('Configuration "%s" not found in this workspace' % config_name)
        config = configs[config_name]
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            raise ValueError("expression and etype must BOTH be None or a string value")
        if etype is None:
            etype = config['rootEntityType']
        response = dog.firecloud.api.get_entity(
            self.namespace,
            self.workspace,
            etype,
            entity
        )
        if response.status_code >= 400 and response.status_code < 500:
            raise TypeError("No such %s '%s' in this workspace. Check your entity and entity type" % (
                etype,
                entity
            ))
        elif response.status_code >= 500:
            raise APIException("The Firecloud API has returned status %d : %s" % (response.status_code, response.text))
        with capture() as (stdout, stderr):
            with dalmatian_api():
                super().create_submission(
                    config['namespace'],
                    config['name'],
                    entity,
                    etype,
                    expression=expression,
                    use_callcache=use_cache
                )
            stdout.seek(0,0)
            stdout_text = stdout.read()
            stderr.seek(0,0)
            stderr_text = stderr.read()
        result = re.search(
            r'Successfully created submission (.+)\.',
            stdout_text
        )
        if result is None:
            raise APIException("Unexpected response from dalmatian: "+stdout_text)
        return result.group(1)

    def get_submission(self, submission_id):
        """
        Gets submission metadata from a lapdog or firecloud submission
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).get_submission(sid)
        elif lapdog_id_pattern.match(submission_id):
            try:
                adapter = self.get_adapter(submission_id)
                return {
                    **adapter.data,
                    # **{
                    #     'status': adapter.submission_status(True)
                    # }
                }
            except:
                with dalmatian_api():
                    return super().get_submission(submission_id)
        with dalmatian_api():
            return super().get_submission(submission_id)

    @parallelize(5)
    def _get_multiple_executions(self, execution_path):
        result = lapdog_submission_pattern.match(execution_path.name)
        if result:
            return self.get_submission(result.group(1))


    def list_configs(self):
        return self.operator.configs

    def get_adapter(self, submission_id):
        """
        Returns a submission adapter for a lapdog submission
        """
        return adapters.SubmissionAdapter(self.get_bucket_id(), submission_id)

    def list_submissions(self, config=None):
        """
        Lists submissions in the workspace
        """
        with dalmatian_api():
            results = super().list_submissions(config)
        for submission in WorkspaceManager._get_multiple_executions(repeat(self), storage.Client().get_bucket(self.get_bucket_id()).list_blobs(prefix='lapdog-executions')):
            if submission is not None:
                results.append(submission)
        return results

    def execute_preflight(self, config_name, entity, expression=None, etype=None):
        """
        Verifies execution configuration
        """
        configs = {config['name']:config for config in self.list_configs()}
        if config_name not in configs:
            return False, 'Configuration "%s" not found in this workspace' % config_name
        config = configs[config_name]
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            return False, "expression and etype must BOTH be None or a string value"
        if etype is None:
            etype = config['rootEntityType']
        entities = self.operator.get_entities_df(etype)
        if entity not in entities.index:
            return False, "No such %s '%s' in this workspace. Check your entity and entity type" % (
                etype,
                entity
            )

        workflow_entities = self.operator.evaluate_expression(
            etype,
            entity,
            (expression if expression is not None else 'this')+'.%s_id' % config['rootEntityType']
        )
        if isinstance(workflow_entities, dict) and 'statusCode' in workflow_entities and workflow_entities['statusCode'] >= 400:
            return False, workflow_entities['message'] if 'message' in workflow_entities else repr(workflow_entities)
        elif not len(workflow_entities):
            return False, "Expression evaluates to 0 entities"

        template = self.operator.get_config_detail(
            config['namespace'],
            config['name']
        )['inputs']

        invalid_inputs = self.operator.validate_config(
            config['namespace'],
            config['name']
        )
        invalid_inputs = {**invalid_inputs['invalidInputs'], **{k:'N/A' for k in invalid_inputs['missingInputs']}}

        return True, config, entity, etype, workflow_entities, template, invalid_inputs


    def execute(self, config_name, entity, expression=None, etype=None, zone='us-east1-b', force=False):
        """
        Validates config parameters then executes a job directly on GCP
        """
        configs = {config['name']:config for config in self.list_configs()}
        if config_name not in configs:
            raise KeyError('Configuration "%s" not found in this workspace' % config_name)
        config = configs[config_name]
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            raise ValueError("expression and etype must BOTH be None or a string value")
        if etype is None:
            etype = config['rootEntityType']
        entities = self.operator.get_entities_df(etype)
        if entity not in entities.index:
            raise TypeError("No such %s '%s' in this workspace. Check your entity and entity type" % (
                etype,
                entity
            ))

        workflow_entities = self.operator.evaluate_expression(
            etype,
            entity,
            (expression if expression is not None else 'this')+'.%s_id' % config['rootEntityType']
        )

        template = self.operator.get_config_detail(
            config['namespace'],
            config['name']
        )['inputs']

        invalid_inputs = self.operator.validate_config(
            config['namespace'],
            config['name']
        )
        invalid_inputs = {**invalid_inputs['invalidInputs'], **{k:'N/A' for k in invalid_inputs['missingInputs']}}

        if len(invalid_inputs):
            if not force:
                raise ValueError("The following inputs are invalid on this configuation: %s" % repr(list(invalid_inputs)))
            else:
                print("The following inputs are invalid on this configuation: %s" % repr(list(invalid_inputs)), file=sys.stderr)

        submission_id = md5((str(time.time()) + config['name'] + entity).encode()).hexdigest()
        global_id = 'lapdog/'+base64.b64encode(
            ('%s/%s/%s' % (self.namespace, self.workspace, submission_id)).encode()
        ).decode()

        print("Global submission ID:", global_id)
        print("Workspace submission ID:", submission_id)

        print("This will launch", len(workflow_entities), "workflow(s)")

        if not force:
            print("Ready to launch workflow(s). Press Enter to continue")
            try:
                input()
            except KeyboardInterrupt:
                print("Aborted", file=sys.stderr)
                return

        submission_data = {
            'workspace':self.workspace,
            'namespace':self.namespace,
            'identifier':global_id,
            'submission_id': submission_id,
            'methodConfigurationName':config['name'],
            'methodConfigurationNamespace':config['namespace'],
            'status': 'Running',
            'submissionDate': time.strftime(timestamp_format),
            'submissionEntity': {
                'entityName': entity,
                'entityType': etype
            },
            'submitter': 'labdog',
            'workflowEntityType': config['rootEntityType'],
            'workflowExpression': expression if expression is not None else None
        }

        @parallelize(5)
        def prepare_workflow(workflow_entity):
            wf_template = {}
            for k,v in template.items():
                if len(v):
                    resolution = self.operator.evaluate_expression(
                        config['rootEntityType'],
                        workflow_entity,
                        v
                    )
                    if len(resolution) == 1:
                        wf_template[k] = resolution[0]
                    else:
                        wf_template[k] = resolution
            return wf_template

        tempdir = tempfile.TemporaryDirectory()
        with open(os.path.join(tempdir.name, 'method.wdl'),'w') as w:
            with dalmatian_api():
                w.write(self.operator.get_wdl(
                    config['methodRepoMethod']['methodNamespace'],
                    config['methodRepoMethod']['methodName']
                ))
        with open(os.path.join(tempdir.name, 'options.json'), 'w') as w:
            json.dump(
                {
                    'default_runtime_attributes': {
                        'zones': zone,
                    },
                    'write_to_cache': True,
                    'read_from_cache': True,
                },
                w
            )

        workflow_inputs = [*status_bar.iter(
            prepare_workflow(workflow_entities),
            len(workflow_entities),
            prepend="Preparing Workflows... "
        )]

        with open(os.path.join(tempdir.name, 'config.json'), 'w') as w:
            json.dump(
                workflow_inputs,
                w,
                # indent='\t'
            )

        submission_data['workflows'] = [
            {
                'workflowEntity': e,
                'workflowOutputKey': build_input_key(t)
            }
            for e, t in zip(workflow_entities, workflow_inputs)
        ]

        submission_data_path = os.path.join(
            'gs://'+self.get_bucket_id(),
            'lapdog-executions',
            submission_id,
            'submission.json'
        )

        cmd = (
            'gcloud alpha genomics pipelines run '
            '--pipeline-file {source_dir}/wdl_pipeline.yaml '
            '--zones {zone} '
            '--inputs-from-file WDL={wdl_text} '
            '--inputs-from-file WORKFLOW_INPUTS={workflow_template} '
            '--inputs-from-file WORKFLOW_OPTIONS={options_template} '
            '--inputs LAPDOG_SUBMISSION_ID={submission_id} '
            '--inputs WORKSPACE=gs://{bucket_id}/lapdog-executions/{submission_id}/workspace '
            '--inputs OUTPUTS=gs://{bucket_id}/lapdog-executions/{submission_id}/results '
            '--inputs SUBMISSION_DATA_PATH={submission_data_path} '
            '--logging gs://{bucket_id}/lapdog-executions/{submission_id}/logs '
            '--labels lapdog-submission-id={submission_id},lapdog-execution-role=cromwell '
            '--service-account-scopes=https://www.googleapis.com/auth/devstorage.read_write'
        ).format(
            source_dir=os.path.dirname(__file__),
            zone=zone,
            wdl_text=os.path.join(tempdir.name, 'method.wdl'),
            workflow_template=os.path.join(tempdir.name, 'config.json'),
            options_template=os.path.join(tempdir.name, 'options.json'),
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id,
            submission_data_path=submission_data_path
        )

        results = subprocess.run(
            cmd, shell=True, executable='/bin/bash',
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        ).stdout.decode()

        try:
            submission_data['operation'] = re.search(
                r'(operations/\S+)\].',
                results
            ).group(1)
        except AttributeError:
            print(results)
            raise

        print("Created submission", global_id)

        getblob(submission_data_path).upload_from_string(json.dumps(submission_data))

        return global_id, submission_id, submission_data['operation']

    def get_submission_cost(self, submission_id):
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).get_submission_cost(sid)
        elif lapdog_id_pattern.match(submission_id):
            return self.get_adapter(submission_id).cost()
        raise TypeError("complete_execution not available for firecloud submissions")

    def complete_execution(self, submission_id):
        """
        Checks a GCP job status and returns results to firecloud, if possible
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).complete_execution(sid)
        elif lapdog_id_pattern.match(submission_id):
            submission = self.get_adapter(submission_id).data
            status = get_operation_status(submission['operation'])
            done = 'done' in status and status['done']
            if done:
                print("All workflows completed. Uploading results...")
                output_template = check_api(dog.firecloud.api.get_workspace_config(
                    submission['namespace'],
                    submission['workspace'],
                    submission['methodConfigurationNamespace'],
                    submission['methodConfigurationName']
                )).json()['outputs']

                output_data = {}
                try:
                    workflow_metadata = json.loads(getblob(
                        'gs://{bucket_id}/lapdog-executions/{submission_id}/results/workflows.json'.format(
                            bucket_id=self.get_bucket_id(),
                            submission_id=submission_id
                        )
                    ).download_as_string())
                except:
                    raise FileNotFoundError("Unable to locate the tracking file for this submission. It may not have finished")

                workflow_metadata = {
                    build_input_key(meta['workflow_metadata']['inputs']):meta
                    for meta in workflow_metadata
                }
                submission_workflows = {wf['workflowOutputKey']: wf['workflowEntity'] for wf in submission['workflows']}
                for key, entity in status_bar.iter(submission_workflows.items(), prepend="Uploading results... "):
                    if key not in workflow_metadata:
                        print("Entity", entity, "has no output metadata")
                    elif workflow_metadata[key]['workflow_status'] != 'Succeeded':
                        print("Entity", entity, "failed")
                        print("Errors:")
                        for call, calldata in workflow_metadata[key]['workflow_metadata']['calls'].items():
                            print("Call", call, "failed with error:", get_operation_status(calldata['jobId'])['error'])
                    else:
                        output_data = workflow_metadata[key]['workflow_output']
                        entity_data = {}
                        for k,v in output_data['outputs'].items():
                            k = output_template[k]
                            if k.startswith('this.'):
                                entity_data[k[5:]] = v
                        with capture():
                            self.update_entity_attributes(
                                submission['workflowEntityType'],
                                pd.DataFrame(
                                    entity_data,
                                    index=[entity]
                                ),
                            )
                return True
            else:
                print("This submission has not finished")
                return False
        raise TypeError("complete_execution not available for firecloud submissions")
