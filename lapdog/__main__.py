import argparse
import dalmatian
import pandas as pd
import json
import os
import numpy as np
import sys
from google.cloud import storage
import csv
from agutil.parallel import parallelize, parallelize2
from agutil import status_bar, byteSize
from hashlib import md5
import time
import tempfile
import subprocess
from datetime import datetime
from itertools import repeat
import re
from io import StringIO
import yaml
import contextlib

@contextlib.contextmanager
def capture_stdout(out, err=True):
    try:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = out
        if err is True:
            old_stderr = sys.stderr
            sys.stderr = out
        elif err is not None:
            old_stderr = sys.stderr
            sys.stderr = err
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr

@contextlib.contextmanager
def capture():
    try:
        stdout_buff = StringIO()
        stderr_buff = StringIO()
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout_buff
        sys.stderr = stderr_buff
        yield (stdout_buff, stderr_buff)
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr

data_path = os.path.join(
    os.path.expanduser('~'),
    '.lapdog'
)

def load_data():
    try:
        with open(data_path) as reader:
            return json.load(reader)
    except FileNotFoundError:
        return {}

def workspaceType(text):
    """
    Text should either be a lapdog workspace name (no '/')
    or a firecloud namespace/workspace path
    """
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    if '/' not in text:
        if text not in data['workspaces']:
            sys.exit("Workspace '%s' is not recognized" % text)

        return dalmatian.WorkspaceManager(
            data['workspaces'][text]['namespace'],
            data['workspaces'][text]['workspace']
        )
    else:
        namespace,workspace = text.split('/')
        return dalmatian.WorkspaceManager(
            namespace, workspace
        )

@parallelize2()
def upload(bucket, path, source):
    blob = bucket.blob(path)
    # print("Commencing upload:", source)
    blob.upload_from_filename(source)

@parallelize2()
def download(blob, local_path):
    blob.download_to_filename(local_path)

def gs_copy(source, dest, project=None, user_project=None, move=False):
    import shutil
    from google.cloud import storage
    import os
    def fetch_blob(path):
        components = path[5:].split('/')
        bucket = storage.Client(
            project=project
        ).bucket(
            components[0],
            user_project=user_project
        )
        if not bucket.exists():
            bucket.create(project=project)
        return bucket.blob(
            '/'.join(components[1:])
        )
    source_local = not source.startswith('gs://')
    dest_local = not source.startswith('gs://')
    if source_local and dest_local:
        shutil.copyfile(source, dest)
    elif not (source_local or dest_local):
        source = fetch_blob(source)
        dest = fetch_blob(dest)
        dest.rewrite(source)
    elif source_local:
        fetch_blob(dest).upload_from_filename(
            source
        )
    else:
        source = fetch_blob(source)
        source.download_from_filename(
            dest
        )
    if move:
        if source_local:
            os.remove(source)
        else:
            source.delete()


def getblob(gs_path):
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        storage.Client().get_bucket(bucket_id)
    )

def main():
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        'workspace',
        type=workspaceType,
        help="Lapdog workspace alias (see workspace subcommand)\n"
        "Or Firecloud workspace in 'namespace/workspace' format",
    )

    exec_parent = argparse.ArgumentParser(add_help=False)
    exec_parent.add_argument(
        'config',
        help="Configuration to run"
    )
    exec_parent.add_argument(
        'entity',
        help="The entity to run on. Entity is assumed to be of the same "
        "type as the configuration's root entity type. If you would like to "
        "run on a different entity type, use the --expression argument"
    )
    exec_parent.add_argument(
        '-x', '--expression',
        nargs=2,
        help="If the entity provided is not the same as the root entity type of"
        " the configuration, use this option to set a different entity type and"
        " entity expression. This option takes two arguments provide "
        "the new entity type followed by the expression for this entity",
        metavar=("ENTITY_TYPE", "EXPRESSION"),
        default=None
    )

    parser = argparse.ArgumentParser(
        'lapdog',
        description="Command line interface to dalmatian and firecloud"
    )
    subparsers = parser.add_subparsers(metavar='<subcommand>')

    ws_parser = subparsers.add_parser(
        'workspace',
        help="Registers a workspace with lapdog",
        description="Registers a workspace with lapdog"
    )
    ws_parser.set_defaults(func=cmd_add_workspace)
    ws_parser.add_argument(
        'namespace',
        help="Namespace the workspace is under"
    )
    ws_parser.add_argument(
        'workspace',
        help="Name of the workspace"
    )
    ws_parser.add_argument(
        'alias',
        nargs='*',
        help="Optional aliases for the workspace"
    )
    ws_parser.add_argument(
        '-c', '--create',
        nargs='?',
        type=workspaceType,
        help="Create a new workspace. Default behavior without this flag is to"
        " fail if the workspace doesn't already exist in firecloud. You can "
        "optionally provide the name of a workspace (in lapdog) as an argument"
        " to this flag, and the new workspace will be cloned from the provided"
        " one",
        metavar="SOURCE",
        default=False,
        const=None
    )
    ws_parser.add_argument(
        '-n', '--no-save',
        action='store_true',
        help="Do not save any aliases. This is only useful for creating new workspaces."
        " Using -n without -c results in a no-op"
    )

    stats_parser = subparsers.add_parser(
        'stats',
        help="Collect stats for a configuration in the workspace",
        description="Collect stats for a configuration in the workspace",
        parents=[parent]
    )
    stats_parser.set_defaults(func=cmd_stats)
    stats_parser.add_argument(
        'configuration',
        help="Configuration to check"
    )

    upload_parser = subparsers.add_parser(
        'upload',
        help="Uploads participant data to a workspace",
        description="Uploads participant data to a workspace",
        parents=[parent]
    )
    upload_parser.set_defaults(func=cmd_upload)
    upload_parser.add_argument(
        'source',
        type=argparse.FileType('r'),
        help="CSV, TSV,  or json file to upload. CSV or TSV must have a header and must include"
        " sample_id and participant_id fields. JSON file must be an array of dicts"
        " or a dict of arrays. In either JSON schema, the dicts must contain "
        "sample_id and participant_id fields."
    )
    upload_parser.add_argument(
        '-f', '--files',
        action='store_true',
        help="Anything that looks like a local filepath will be uploaded to the"
        " workspace's bucket prior to uploading the samples"
    )

    method_parser = subparsers.add_parser(
        'method',
        help="Uploads a method or config to firecloud",
        description="Uploads a method or config to firecloud",
        parents=[parent]
    )
    method_parser.set_defaults(func=cmd_method)
    method_parser.add_argument(
        '-w', '--wdl',
        type=argparse.FileType('r'),
        help="WDL to upload",
        default=None
    )
    method_parser.add_argument(
        '-n', '--method-name',
        help="The name of the uploaded method. This argument is ignored if the"
        " --wdl argument is not provided. By default, if a method configuration"
        " is provided, this equals the methodRepoMethod.methodName key."
        " If no method configuration is provided, this will default to the"
        " filename (without extensions) of the provided WDL",
        default=None
    )
    method_parser.add_argument(
        '-a', '--namespace',
        help="The namespace to upload the method and configuration."
        " By default, if a method configuration is provided, this equals"
        " the namespace from the methodRepoMethod.methodNamespace key."
        " If no method configuration is provided, this will default to the"
        " same namespace as the current workspace",
        default=None
    )
    method_parser.add_argument(
        '-c', '--config',
        type=argparse.FileType('r'),
        help="Configuration to upload. If methodRepoMethod.methodVersion"
        " is set to 'latest', the version will be set to the latest method snapshot"
        " including a new upload with the --wdl argument",
        default=None
    )

    attributes_parser = subparsers.add_parser(
        'attributes',
        help="Sets attributes on the workspace",
        description="Sets attributes on the workspace",
        parents=[parent]
    )
    attributes_parser.set_defaults(func=cmd_attrs)
    attributes_parser.add_argument(
        'source',
        type=argparse.FileType('r'),
        help="JSON file to upload. The root object must be a dictionary"
    )
    attributes_parser.add_argument(
        '-f', '--files',
        action='store_true',
        help="Anything that looks like a local filepath will be uploaded to the"
        " workspace's bucket prior to uploading the samples"
    )

    exec_parser = subparsers.add_parser(
        'exec',
        help='Executes a configuration outside of firecloud. '
        'Execution will occur directly on GCP and outputs will be returned to firecloud',
        description='Executes a configuration outside of firecloud. '
        'Execution will occur directly on GCP and outputs will be returned to firecloud',
        parents=[parent, exec_parent]
    )
    exec_parser.set_defaults(func=cmd_exec)
    exec_parser.add_argument(
        '-z', '--zone',
        help="Execution Zone to use. Default: 'us-east1-b'",
        default='us-east1-b'
    )

    run_parser = subparsers.add_parser(
        'run',
        aliases=['submit'],
        help='Submits a job to run in firecloud',
        description='Submits a job to run in firecloud',
        parents=[parent, exec_parent],
        conflict_handler='resolve'
    )
    run_parser.set_defaults(func=cmd_run)
    run_parser.add_argument(
        '-n', '--no-cache',
        action='store_true',
        help="Disables the use of the call cache in firecloud"
    )
    run_parser.add_argument(
        '-a', '--after',
        help="Do not run the submission immediately, and schedule it after the "
        "provided submission ID. You may alternatively provide a temporary id "
        "provided by lapdog to schedule this submission after one that has not "
        "yet started.",
        metavar="SUBMISSION_ID",
        default=None
    )

    sub_parser = subparsers.add_parser(
        'submissions',
        help="Get status of submissions in the workspace",
        description="Get status of submissions in the workspace",
        parents=[parent]
    )
    sub_parser.set_defaults(func=cmd_submissions)
    sub_parser.add_argument(
        '-i', '--id',
        # action='append',
        help="Display only the submission with this id",
        metavar='SUBMISSION_ID',
        default=None
    )
    sub_parser.add_argument(
        '-c', '--config',
        # action='append',
        help='Display only submissions with this configuration',
        default=None
    )
    sub_parser.add_argument(
        '-e', '--entity',
        # action='append',
        help="Display only submissions on this entity",
        default=None
    )
    sub_parser.add_argument(
        '-d', '--done',
        action='store_true',
        help="Display only submissions which have finished"
    )

    config_parser = subparsers.add_parser(
        'configurations',
        aliases=['configs'],
        help="List all configurations in the workspace",
        description="List all configurations in the workspace",
        parents=[parent]
    )
    config_parser.set_defaults(func=cmd_configs)

    list_parser = subparsers.add_parser(
        'list',
        help="List all workspaces known to lapdog",
        description="List all workspaces known to lapdog"
    )
    list_parser.set_defaults(func=cmd_list)

    info_parser = subparsers.add_parser(
        'info',
        help="Display summary statistics for the workspace",
        description="Display summary statistics for the workspace",
        parents=[parent]
    )
    info_parser.set_defaults(func=cmd_info)

    finish_parser = subparsers.add_parser(
        'finish',
        help="Finishes an execution and uploads results to firecloud",
        description="Finishes an execution and uploads results to firecloud",
    )
    finish_parser.set_defaults(func=cmd_finish)
    finish_parser.add_argument(
        'submission',
        help="Lapdog submission id provided by 'lapdog exec'"
    )
    finish_parser.add_argument(
        '-s','--status',
        action='store_true',
        help="Show the status of the submission and exit without doing anything"
    )
    finish_parser.add_argument(
        '-a', '--abort',
        action='store_true',
        help="Abort the submission, if it hasn't finished already"
    )

    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_usage()
        sys.exit("You must provide a valid subcommand")
    func(args)

def cmd_add_workspace(args):
    if args.no_save and not args.create:
        print("Warning: Using --no-save without --create results in a no-op")
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    if args.workspace in data['workspaces']:
        sys.exit('This workspace already exists')

    for name in [args.workspace] + args.alias:
        if '/' in name:
            sys.exit("Workspace name '%s' cannot contain '/'" % name)

    if not args.no_save:
        valid = [alias for alias in args.alias if alias not in data['workspaces']]
        if len(args.alias) and not len(valid):
            sys.exit('None of the provided aliases were available')
        elif len(valid) < len(args.alias):
            print("Warning: Not all aliases were available")

    ws = dalmatian.WorkspaceManager(args.namespace, args.workspace)
    try:
        ws.get_bucket_id()
        exists = True
    except AssertionError:
        exists = False
    if exists and args.create is not False:
        sys.exit("This workspace already exists")
    elif args.create is False and not exists:
        sys.exit("This workspace does not exist. Use the --create flag to create a new one")

    if args.create is not False:
        #args.create will either be a workspace type or None
        ws.create_workspace(wm=args.create)
    if not args.no_save:
        data = load_data()
        if 'workspaces' not in data:
            data['workspaces'] = {}
        for name in valid + [args.workspace]:
            data['workspaces'][name] = {
                'namespace': args.namespace,
                'workspace': args.workspace
            }
        with open(data_path, 'w') as writer:
            json.dump(data, writer, indent='\t')

def cmd_stats(args):
    try:
        print("Retrieving submission history...")
        sample_status = args.workspace.get_entity_status(None, args.configuration)
        print("Configuration name: ", np.unique(sample_status['configuration']))
        print("Gathering latest submission data...")
        status,_ = args.workspace.get_stats(sample_status)
        for sample, row in status.iterrows():
            if row.status != 'Succeeded':
                print(sample,row.status)
        print("Total Runtime:", max(status['time_h']), 'hours')
        print("Total CPU time:", sum(status['cpu_hours']), 'hours')
        print("Estimated cost: $", sum(status.query('est_cost == est_cost')['est_cost']), sep='')
    except AssertionError:
        # raise
        sys.exit("lapdog has encountered an error with firecloud. Please try again later")
    except:
        raise
        sys.exit("lapdog encountered an unexpected error")

def cmd_upload(args):
    """
    CSV or json file to upload. CSV must have a header and must include
    sample_id and participant_id fields. JSON file must be an array of dicts
    or a dict of arrays. In either JSON schema, the dicts must contain
    sample_id and participant_id fields.
    """

    if args.source.name.endswith('.csv') or args.source.name.endswith('.tsv'):
        reader = csv.DictReader(
            args.source,
            delimiter='\t' if args.source.name.endswith('.tsv') else ','
        )
        if 'sample_id' not in reader.fieldnames or 'participant_id' not in reader.fieldnames:
            sys.exit("Input source file must contain 'sample_id' and 'participant_id' fields")
        if args.files:
            samples = []
            pending_uploads = []
            bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
            (root, ext) = os.path.splitext(args.source.name)
            with open(root+'.lapdog'+ext, 'w') as w:
                writer = csv.DictWriter(w, reader.fieldnames, delimiter='\t' if args.source.name.endswith('.tsv') else ',', lineterminator='\n')
                writer.writeheader()
                for sample in reader:
                    for k,v in sample.items():
                        if os.path.isfile(v):
                            bucket_path = 'samples/%s/%s' % (
                                sample['sample_id'],
                                os.path.basename(v)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", v, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, v))
                            sample[k] = gs_path
                    writer.writerow(sample)
                    samples.append({k:v for k,v in sample.items()})
            _ = [callback() for callback in status_bar.iter(pending_uploads)]
        else:
            samples = list(reader)
        # args.workspace.upload_data(samples)
        args.workspace.upload_samples(pd.DataFrame(samples).set_index('sample_id'), add_participant_samples=True)
    elif args.source.name.endswith('.json'):
        source = json.load(args.source)
        if type(source) == list and type(souce[0]) == dict and 'sample_id' in source[0] and 'participant_id' in source[0]:
            #standard
            if args.files:
                pending_uploads = []
                bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
                for i in len(source):
                    sample = source[i]
                    for k,v in sample.items():
                        if os.path.isfile(v):
                            bucket_path = 'samples/%s/%s' % (
                                sample['sample_id'],
                                os.path.basename(v)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", v, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, v))
                            source[i][k] = gs_path
                (root, ext) = os.path.splitext(args.source.name)
                with open(root+'.lapdog'+ext, 'w') as w:
                    json.dump(source, w, indent='\t')
                _ = [callback() for callback in status_bar.iter(pending_uploads)]
        elif type(source) == dict and type(source[[k for k in source][0]]) == list and 'sample_id' in source and 'participant_id' in source:
            if args.files:
                pending_uploads = []
                bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
                for key in source:
                    for i in range(len(source[key])):
                        entry = source[key][i]
                        if os.path.isfile(entry):
                            bucket_path = 'samples/%s/%s' % (
                                source['sample_id'][i],
                                os.path.basename(entry)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", entry, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, entry))
                            # blob = bucket.blob(bucket_path)
                            # blob.upload_from_filename(entry)
                            source[key][i] = gs_path
                (root, ext) = os.path.splitext(args.source.name)
                with open(root+'.lapdog'+ext, 'w') as w:
                    json.dump(source, w, indent='\t')
                _ = [callback() for callback in status_bar.iter(pending_uploads)]
        df = pd.DataFrame(source).set_index('sample_id')
        df.columns = ['participant_id'] + [*(set(df.columns)-{'participant_id'})]
        args.workspace.upload_samples(df, add_participant_samples=True)
    else:
        sys.exit("Please use a .tsv, .csv, or .json file")

def cmd_method(args):
    if args.wdl is None and args.config is None:
        sys.exit("Must provide either a method or configuration")

    if args.config is not None:
        args.config = json.load(args.config)
    if args.namespace is None:
        args.namespace = args.config['methodRepoMethod']['methodNamespace'] if args.config is not None else args.workspace.namespace
    if args.method_name is None:
        args.method_name = args.config['methodRepoMethod']['methodName'] if args.config is not None else os.path.splitext(os.path.basename(args.wdl.name))[0]

    version = None
    if args.wdl is not None:
        with capture() as (stdout, stderr):
            dalmatian.update_method(
                args.namespace,
                args.method_name,
                "Runs " + args.method_name,
                args.wdl.name
            )
            stdout.seek(0,0)
            out_text = stdout.read()
            stderr.seek(0,0)
            err_text = stderr.read()
        print(out_text, end='')
        print(err_text, end='', file=sys.stderr)
        result = re.search(r'New SnapshotID: (\d+)', out_text)
        if result:
            version = int(result.group(1))
    if args.config is not None:
        if args.config['methodRepoMethod']['methodVersion'] == 'latest':
            print(
                "Checking most recent version of %s/%s ..." % (
                    args.config['methodRepoMethod']['methodNamespace'],
                    args.config['methodRepoMethod']['methodName']
                )
            )
            args.config['methodRepoMethod']['methodVersion'] = version if version is not None else int(dalmatian.get_method_version(
                args.config['methodRepoMethod']['methodNamespace'],
                args.config['methodRepoMethod']['methodName']
            ))
            print("Detected version", args.config['methodRepoMethod']['methodVersion'])
        args.workspace.update_configuration(args.config)

def cmd_attrs(args):

    source = json.load(args.source)
    if type(source) != dict:
        sys.exit("The root object must be a dictionary")
    if args.files:
        pending_uploads = []
        bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
        source, pending_uploads = walk_and_upload(bucket, source)
        (root, ext) = os.path.splitext(args.source.name)
        with open(root+'.lapdog'+ext, 'w') as w:
            json.dump(source, w, indent='\t')
        _ = [callback() for callback in status_bar.iter(pending_uploads)]
    args.workspace.update_attributes(source)

def walk_and_upload(bucket, obj):
    output = []
    if type(obj) == dict:
        for key,val in obj.items():
            if type(val) in {dict, list}:
                obj[key], tmp = walk_and_upload(bucket, val)
                output += tmp
            elif type(val) == str and os.path.isfile(val):
                bucket_path = 'workspace/%s' % (
                    os.path.basename(val)
                )
                gs_path = 'gs://%s/%s' % (
                    bucket.id,
                    bucket_path
                )
                output.append(upload(bucket, bucket_path, val))
                obj[key] = gs_path
    elif type(obj) == list:
        for key in range(len(obj)):
            val = obj[key]
            if type(val) in {dict, list}:
                obj[key], tmp = walk_and_upload(bucket, prefix, val)
                output += tmp
            elif type(val) == str and os.path.isfile(val):
                bucket_path = 'workspace/%s' % (
                    os.path.basename(val)
                )
                gs_path = 'gs://%s/%s' % (
                    bucket.id,
                    bucket_path
                )
                output.append(upload(bucket, bucket_path, val))
                obj[key] = gs_path
    return (obj, output)

def cmd_run(args):
    configs = {
        config['name']:config for config in
        dalmatian.firecloud.api.list_workspace_configs(
            args.workspace.namespace,
            args.workspace.workspace
        ).json()
    }
    if args.config not in configs:
        print(
            "Configurations found in this workspace:",
            [config for config in configs]
        )
        sys.exit("Configuration '%s' does not exist" % args.config)
    args.config = configs[args.config]
    etype = args.expression[0] if args.expression is not None else args.config['rootEntityType']
    response = dalmatian.firecloud.api.get_entity(
        args.workspace.namespace,
        args.workspace.workspace,
        etype,
        args.entity
    )
    if response.status_code >= 400 and response.status_code <500:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
    elif response.status_code >= 500:
        sys.exit("Encountered an unexpected error with the firecloud api")
    data = load_data()
    tmp_id = 'tmp::'+md5(str(time.time()).encode()).hexdigest()
    if 'submissions' in data:
        while tmp_id in data['submissions']:
            tmp_id = 'tmp::'+md5(tmp_id.encode()).hexdigest()
    if args.after is not None:
        print("Temporary ID:", tmp_id)
        print(
            "You may use that in place of a submission id to schedule submissions"
            " after this one"
        )
        if args.after.startswith('tmp::'):
            print("Waiting for lapdog to start the submission for ", args.after)
            count = 0
            while not ('submissions' in data and args.after in data['submissions']):
                if count < 5:
                    time.sleep(60)
                elif count < 10:
                    time.sleep(120)
                elif count < 20:
                    time.sleep(300)
                else:
                    time.sleep(1800)
                count += 1
                data = load_data()
            print(
                "The submission for",
                args.after,
                "has been started:",
                data['submissions'][args.after]
            )
            args.after = data['submissions'][args.after]
        response = dalmatian.firecloud.api.get_submission(
            args.workspace.namespace,
            args.workspace.workspace,
            args.after
        )
        if response.status_code not in {200,201}:
            sys.exit("Failed to find submission ID: "+response.text)
        running = response.json()['status'] != 'Done'
        succeeded = True
        for workflow in response.json()['workflows']:
            if workflow['status'] != 'Succeeded':
                succeeded = False
        if not (running or succeeded):
            sys.exit("The provided workflow ID has failed")
        elif running:
            print("Waiting for the submission to finish")
            count = 0
            while running:
                if count < 5:
                    time.sleep(60)
                elif count < 10:
                    time.sleep(120)
                elif count < 20:
                    time.sleep(300)
                else:
                    time.sleep(1800)
                count += 1
                response = dalmatian.firecloud.api.get_submission(
                    args.workspace.namespace,
                    args.workspace.workspace,
                    args.after
                )
                if response.status_code not in {200,201}:
                    sys.exit("Failed to find submission ID: "+response.text)
                running = response.json()['status'] != 'Done'
                succeeded = True
                for workflow in response.json()['workflows']:
                    if workflow['status'] != 'Succeeded':
                        succeeded = False
        if not succeeded:
            sys.exit("The provided workflow ID has failed")
    print("Creating submission")
    with capture() as (stdout, stderr):
        args.workspace.create_submission(
            args.config['namespace'],
            args.config['name'],
            args.entity,
            etype,
            expression=args.expression[1] if args.expression is not None else None,
            use_callcache=not args.no_cache
        )
        stdout.seek(0,0)
        stdout_text = stdout.read()
        stderr.seek(0,0)
        stderr_text = stderr.read()
    print(stdout_text, end='')
    print(stderr_text, end='', file=sys.stderr)
    result = re.search(
        r'Successfully created submission (.+)\.',
        stdout_text
    )
    if result is None:
        sys.exit("Failed to create submission")
    result = result.group(1)
    data = load_data()
    if 'submissions' not in data:
        data['submissions'] = {}
    data['submissions'][tmp_id]=result
    with open(data_path, 'w') as writer:
        json.dump(data, writer, indent='\t')

def cmd_submissions(args):
    submissions = args.workspace.get_submission_status(filter_active=args.done)
    if args.id:
        submissions=submissions[
            submissions['submission_id'] == args.id
        ]
    if args.config:
        submissions=submissions[
            submissions['configuration'] == args.config
        ]
    if args.entity:
        submissions=submissions[
            submissions.index == args.entity
        ]
    print(submissions)

def cmd_configs(args):

    print("Configurations in this workspace:")
    print("Configuration\tSynopsis")
    for config in dalmatian.firecloud.api.list_workspace_configs(args.workspace.namespace, args.workspace.workspace).json():
        config_data = dalmatian.firecloud.api.get_repository_method(
            config['methodRepoMethod']['methodNamespace'],
            config['methodRepoMethod']['methodName'],
            config['methodRepoMethod']['methodVersion']
        ).json()
        print(
            config['name'],
            config_data['synopsis'] if 'synopsis' in config_data else '',
            sep='\t'
        )

def cmd_list(args):
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    workspaces = {}
    for alias, workspace in data['workspaces'].items():
        namespace = workspace['namespace']
        workspace = workspace['workspace']
        if namespace not in workspaces:
            workspaces[namespace] = {workspace: [alias]}
        elif workspace not in workspaces[namespace]:
            workspaces[namespace][workspace] = [alias]
        else:
            workspaces[namespace][workspace].append(alias)

    print("Firecloud Workspace\tLapdog Workspace Names")
    print()
    for namespace in workspaces:
        for workspace in workspaces[namespace]:
            first = True
            name = '%s/%s'%(namespace, workspace)
            for alias in workspaces[namespace][workspace]:
                print('%s\t%s'%(
                    name if first else ' '*len(name),
                    alias
                ))
                first = False

def cmd_info(args):

    submissions = args.workspace.get_submission_status(filter_active=False)
    bins = {}
    for row in submissions.iterrows():
        entity = row[0]
        row = row[1]
        if row.status not in bins:
            bins[row.status]={row.configuration:[(row.submission_id, entity)]}
        elif row.configuration not in bins[row.status]:
            bins[row.status][row.configuration]=[(row.submission_id, entity)]
        else:
            bins[row.status][row.configuration].append((row.submission_id, entity))

    # print("Lapdog Workspace:",args.workspace)
    print("Firecloud Workspace:", '%s/%s' % (args.workspace.namespace, args.workspace.workspace))
    print("Submissions:")
    for status in bins:
        print(status,sum(len(item) for item in bins[status].values()),"submission(s):")
        for configuration in bins[status]:
            print('\t'+configuration, len(bins[status][configuration]), 'submission(s):')
            for submission, entity in bins[status][configuration]:
                print('\t\t'+submission, '(%s)'%entity)


def build_input_key(template):
    data = ''
    for k in sorted(template):
        if template[k] is not None:
            data += str(template[k])
    return md5(data.encode()).hexdigest()

def cmd_exec(args):
    # 1) resolve configuration
    configs = {
        config['name']:config for config in
        dalmatian.firecloud.api.list_workspace_configs(
            args.workspace.namespace,
            args.workspace.workspace
        ).json()
    }
    if args.config not in configs:
        print(
            "Configurations found in this workspace:",
            [config for config in configs]
        )
        sys.exit("Configuration '%s' does not exist" % args.config)
    args.config = configs[args.config]
    # 2) Validate entity and interpret workflows
    etype = args.expression[0] if args.expression is not None else args.config['rootEntityType']
    response = dalmatian.firecloud.api.get_entity(
        args.workspace.namespace,
        args.workspace.workspace,
        etype,
        args.entity
    )
    if response.status_code >= 400 and response.status_code <500:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
    elif response.status_code >= 500:
        sys.exit("Encountered an unexpected error with the firecloud api")

    print("Resolving wrokflow entities")
    workflow_entities = dalmatian.firecloud.api.__post(
        'workspaces/%s/%s/entities/%s/%s/evaluate' % (
            args.workspace.namespace,
            args.workspace.workspace,
            etype,
            args.entity
        ),
        data=(
            args.expression[1]
            if args.expression is not None
            else 'this'
        )+'.%s_id' % args.config['rootEntityType']
    ).json()
    print("This will launch", len(workflow_entities), "workflow(s)")

    # 3) Prepare config template
    # print("Resolving inputs")

    template = dalmatian.firecloud.api.get_workspace_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['inputs']

    invalid_inputs = dalmatian.firecloud.api.validate_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['invalidInputs']

    if len(invalid_inputs):
        print("The following input fields are invalid:", list(invalid_inputs))

    submission_id = md5((str(time.time()) + args.config['name'] + args.entity).encode()).hexdigest()
    print("Submission ID for this job:", submission_id)
    # print("Debug info:")
    # print("argument template:", json.dumps(template, indent='\t'))
    # print("Config entity type:", etype)
    # print("Entities:", workflow_entities[:5],'...')
    print("Ready to launch workflow(s). Press Enter to continue")
    try:
        input()
    except KeyboardInterrupt:
        sys.exit("Aborted")

    submission_data = {
        'workspace':args.workspace.workspace,
        'namespace':args.workspace.namespace,
        'config':args.config['name'],
        'workflow_entity_type': args.config['rootEntityType'],
        'entity':args.entity,
        'etype':etype,
        'expression':args.expression[1] if args.expression is not None else None
    }

    @parallelize(5)
    def prepare_workflow(args, submission_id, template, invalid, etype, entity):
        workflow_template = {}
        for key, val in template.items():
            if key not in invalid:
                pass
            resolution = dalmatian.firecloud.api.__post(
                'workspaces/%s/%s/entities/%s/%s/evaluate' % (
                    args.workspace.namespace,
                    args.workspace.workspace,
                    etype,
                    entity
                ),
                data=val
            ).json()
            if len(resolution) == 1:
                workflow_template[key] = resolution[0]
            else:
                workflow_template[key] = resolution

        return workflow_template

    tempdir = tempfile.TemporaryDirectory()
    with open(os.path.join(tempdir.name, 'method.wdl'),'w') as w:
        w.write(dalmatian.get_wdl(
            args.config['methodRepoMethod']['methodNamespace'],
            args.config['methodRepoMethod']['methodName']
        ))
    with open(os.path.join(tempdir.name, 'options.json'), 'w') as w:
        json.dump(
            {
                'default_runtime_attributes': {
                    'zones': args.zone,
                },
                'write_to_cache': True,
                'read_from_cache': True,
            },
            w
        )

    workflow_inputs = [*status_bar.iter(prepare_workflow(
        repeat(args),
        repeat(submission_id),
        repeat(template),
        repeat(invalid_inputs),
        repeat(args.config['rootEntityType']),
        workflow_entities
    ), len(workflow_entities), prepend="Preparing Workflows... ")]

    with open(os.path.join(tempdir.name, 'config.json'), 'w') as w:
        json.dump(
            workflow_inputs,
            w,
            # indent='\t'
        )

    submission_data['workflows'] = [
        {
            'entity':entity,
            'key':build_input_key(template)
        }
        for entity, template in zip(workflow_entities, workflow_inputs)
    ]

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
        '--logging gs://{bucket_id}/lapdog-executions/{submission_id}/logs '
        '--labels lapdog-submission-id={submission_id},lapdog-execution-role=cromwell '
        '--service-account-scopes=https://www.googleapis.com/auth/devstorage.read_write'
    ).format(
        source_dir=os.path.dirname(__file__),
        zone=args.zone,
        wdl_text=os.path.join(tempdir.name, 'method.wdl'),
        workflow_template=os.path.join(tempdir.name, 'config.json'),
        options_template=os.path.join(tempdir.name, 'options.json'),
        bucket_id=args.workspace.get_bucket_id(),
        submission_id=submission_id,
    )

    results = subprocess.run(
        cmd, shell=True, executable='/bin/bash',
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    ).stdout.decode()


    submission_data['operation'] = re.search(
        r'(operations/\S+)\].',
        results
    ).group(1)

    data = load_data()
    if 'executions' not in data:
        data['executions'] = {}
    data['executions'][submission_id] = submission_data
    with open(data_path, 'w') as writer:
        json.dump(data, writer, indent='\t')

    print("Done! Use 'lapdog finish %s' to upload the results after the job finishes" %submission_id)

def get_operation_status(opid):
    return yaml.load(
        StringIO(
            subprocess.run(
                'gcloud alpha genomics operations describe %s' % (
                    opid
                ),
                shell=True,
                executable='/bin/bash',
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            ).stdout.decode()
        )
    )

def abort_operation(opid):
    return subprocess.run(
        'yes | gcloud alpha genomics operations cancel %s' % (
            opid
        ),
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

def cmd_finish(args):
    data = load_data()
    if 'executions' not in data:
        data['executions'] = {}
    if args.submission not in data['executions']:
        sys.exit("No such submission: "+args.submission)
    submission = data['executions'][args.submission]


    status = get_operation_status(submission['operation'])
    done = 'done' in status and status['done']

    print("Submission complete:", done)

    if args.status:
        if done:
            ws = dalmatian.WorkspaceManager(
                submission['namespace'],
                submission['workspace']
            )
            try:
                workflow_metadata = json.loads(getblob(
                    'gs://{bucket_id}/lapdog-executions/{submission_id}/results/workflows.json'.format(
                        bucket_id=ws.get_bucket_id(),
                        submission_id=args.submission
                    )
                ).download_as_string())
            except:
                sys.exit("Unable to locate tracking file for this submission. It may have been aborted")
            mtypes = {
                'n1-standard-%d'%(2**i): (0.0475*(2**i), 0.01*(2**i)) for i in range(7)
            }
            mtypes.update({
                'n1-highmem-%d'%(2**i): (.0592*(2**i), .0125*(2**i)) for i in range(1,7)
            })
            mtypes.update({
                'n1-highcpu-%d'%(2**i): (.03545*(2**i), .0075*(2**1)) for i in range(1,7)
            })
            mtypes.update({
                'f1-micro': (0.0076, 0.0035),
                'g1-small': (0.0257, 0.007)
            })
            cost = 0
            maxTime = 0
            total = 0
            for wf in workflow_metadata:
                for calls in wf['workflow_metadata']['calls'].values():
                    for call in calls:
                        if 'end' in call:
                            delta = datetime.strptime(call['end'].split('.')[0], '%Y-%m-%dT%H:%M:%S') - datetime.strptime(call['start'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
                            delta = (delta.days*24) + (delta.seconds/3600)
                            if delta > maxTime:
                                maxTime = delta
                            total += delta
                            if 'jes' in call and 'machineType' in call['jes'] and call['jes']['machineType'].split('/')[-1] in mtypes:
                                cost += mtypes[call['jes']['machineType'].split('/')[-1]][int('preemptible' in call and call['preemptible'])]*delta
            cost += mtypes['n1-standard-1'][0] * maxTime
            print("Total Runtime: %0.2f hours" % maxTime)
            print("Total CPU hours: %0.2f" % total)
            print("Estimated Cost: $%0.2f" %cost)
        return
    if args.abort:
        result = abort_operation(submission['operation'])
        #If we can successfully place lapdog-submission-id and lapdog-execution-role labels
        #on worker VMs, then we can run:
        #gcloud compute instances delte (
        # gcloud compute instances list --filter="label:lapdog-execution-role=worker AND label:lapdod-submission-id={submission_id}" \
        # | awk 'NR>1{print $1}'
        # )
        #Alternatively, have cromwell_driver write a list of workflow ids to gs://.../workspace/workflow_ids.json
        #["id",...]
        #Then abort by getting instances with --filter="label:cromwell-workflow-id=cromwell-{workflow_id}"
        if result.returncode and not ('done' in status and status['done']):
            print("Failed to abort:", result.stdout.decode())
        print("Workflow(s) aborted")
        return
    if done:
        print("All workflows completed. Uploading results...")
        ws = dalmatian.WorkspaceManager(
            submission['namespace'],
            submission['workspace']
        )

        output_template = dalmatian.firecloud.api.get_workspace_config(
            submission['namespace'],
            submission['workspace'],
            'aarong',
            submission['config']
        ).json()['outputs']

        output_data = {}
        try:
            workflow_metadata = json.loads(getblob(
                'gs://{bucket_id}/lapdog-executions/{submission_id}/results/workflows.json'.format(
                    bucket_id=ws.get_bucket_id(),
                    submission_id=args.submission
                )
            ).download_as_string())
        except:
            sys.exit("Unable to locate tracking file for this submission. It may have been aborted")

        workflow_metadata = {
            build_input_key(meta['workflow_metadata']['inputs']):meta
            for meta in workflow_metadata
        }
        submission_workflows = {wf['key']: wf['entity'] for wf in submission['workflows']}
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
                with capture_stdout(StringIO()):
                    ws.update_entity_attributes(
                        submission['workflow_entity_type'],
                        pd.DataFrame(
                            entity_data,
                            index=[entity]
                        ),
                    )

if __name__ == '__main__':
    main()


# Lapdog Execution Caveats
#
# * For the most part, `lapdog exec` was designed to mimic the same interface as `lapdog run`
# * There are the following caveats to this process:
#   * You must add your billing project's service account as a WRITER to any firecloud project you wish to execute on
#     * Furthermore, you must add the service account as a READER to any firecloud project you wish to read data from
#     * Mainly, this means that if you want to work on a cloned database, you need WRITER on your database and READER on the parent
#   * You will be charged for execution (instead of charging the firecloud billing project)
#   * Each submission incurs an overhead of $0.05/hour to run a non-preemptible cromwell server
