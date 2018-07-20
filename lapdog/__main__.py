import argparse
import pandas as pd
import lapdog
import os

def main():
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        'workspace',
        type=lapdog.WorkspaceManager,
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
        '-c', '--create',
        nargs='?',
        type=lapdog.WorkspaceManager,
        help="Create a new workspace. Default behavior without this flag is to"
        " fail if the workspace doesn't already exist in firecloud. You can "
        "optionally provide the name of another workspace as an argument"
        " to this flag, and the new workspace will be cloned from the provided"
        " one",
        metavar="SOURCE",
        default=False,
        const=None
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
    ws = lapdog.WorkspaceManager(args.namespace, args.workspace)
    try:
        ws.get_bucket_id()
        exists = True
    except lapdog.APIException:
        exists = False
    if exists and args.create is not False:
        sys.exit("This workspace already exists")
    elif args.create is False and not exists:
        sys.exit("This workspace does not exist. Use the --create flag to create a new one")

    if args.create is not False:
        #args.create will either be a workspace type or None
        ws.create_workspace(wm=args.create)

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
        df = pd.read_csv(args.source, sep='\t' if args.source.name.endswith('.tsv') else ',')
    elif args.source.name.endswith('.json'):
        df = pd.DataFrame(json.load(args.source))
    else:
        sys.exit("Please use a .tsv, .csv, or .json file")
    if 'sample_id' not in df.columns or participant_id not in df.columns:
        sys.exit("Input source file must contain 'sample_id' and 'participant_id' fields")
    df = df.set_index('sample_id')
    df = df[['participant_id'] + [*{col for col in df.columns if col != 'participant_id'}]]
    if args.files:
        df = args.workspace.prepare_sample_df(df)
        root, ext = os.path.splitext(args.source.name)
        with open(root+'.lapdog'+ext, 'w') as w:
            if ext == '.csv':
                df.to_csv(w)
            elif ext == '.tsv':
                df.to_csv(w, sep='\t')
            elif ext == '.json':
                df.to_json(w)
    args.workspace.upload_samples(df, add_participant_samples=True)

def cmd_method(args):
    if args.wdl is None and args.config is None:
        sys.exit("Must provide either a method or configuration")

    if args.config is not None:
        args.config = json.load(args.config)
    if args.method_name is None:
        args.method_name = os.path.splitext(os.path.basename(args.wdl.name))[0]
    args.workspace.update_configuration(args.config, args.wdl, args.method_name, args.namespaced)

def cmd_attrs(args):
    args.workspace.update_attributes(**json.load(args.source))

def cmd_run(args):
    etype, expr = args.expression if args.expression is not None else (None, None)
    args.workspace.create_submission(
        args.config,
        args.entity,
        expr,
        etype,
        not args.no_cache
    )

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


def cmd_exec(args):
    etype, expr = args.expression if args.expression is not None else (None, None)
    global_id, submission_id, operation_id = args.workspace.execute(
        args.config,
        args.entity,
        expr,
        etype,
        args.zone,
        False
    )
    print()
    print("Done! Use 'lapdog finish %s' to upload the results after the job finishes" %global_id)

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
    print("Note: lapdog-finish is not yet fully implemented")
    lapdog.complete_execution(args.submission_id)

    # if args.status:
    #     if done:
    #         ws = dalmatian.WorkspaceManager(
    #             submission['namespace'],
    #             submission['workspace']
    #         )
    #         try:
    #             workflow_metadata = json.loads(getblob(
    #                 'gs://{bucket_id}/lapdog-executions/{submission_id}/results/workflows.json'.format(
    #                     bucket_id=ws.get_bucket_id(),
    #                     submission_id=args.submission
    #                 )
    #             ).download_as_string())
    #         except:
    #             sys.exit("Unable to locate tracking file for this submission. It may have been aborted")
    #         mtypes = {
    #             'n1-standard-%d'%(2**i): (0.0475*(2**i), 0.01*(2**i)) for i in range(7)
    #         }
    #         mtypes.update({
    #             'n1-highmem-%d'%(2**i): (.0592*(2**i), .0125*(2**i)) for i in range(1,7)
    #         })
    #         mtypes.update({
    #             'n1-highcpu-%d'%(2**i): (.03545*(2**i), .0075*(2**1)) for i in range(1,7)
    #         })
    #         mtypes.update({
    #             'f1-micro': (0.0076, 0.0035),
    #             'g1-small': (0.0257, 0.007)
    #         })
    #         cost = 0
    #         maxTime = 0
    #         total = 0
    #         for wf in workflow_metadata:
    #             for calls in wf['workflow_metadata']['calls'].values():
    #                 for call in calls:
    #                     if 'end' in call:
    #                         delta = datetime.strptime(call['end'].split('.')[0], '%Y-%m-%dT%H:%M:%S') - datetime.strptime(call['start'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
    #                         delta = (delta.days*24) + (delta.seconds/3600)
    #                         if delta > maxTime:
    #                             maxTime = delta
    #                         total += delta
    #                         if 'jes' in call and 'machineType' in call['jes'] and call['jes']['machineType'].split('/')[-1] in mtypes:
    #                             cost += mtypes[call['jes']['machineType'].split('/')[-1]][int('preemptible' in call and call['preemptible'])]*delta
    #         cost += mtypes['n1-standard-1'][0] * maxTime
    #         print("Total Runtime: %0.2f hours" % maxTime)
    #         print("Total CPU hours: %0.2f" % total)
    #         print("Estimated Cost: $%0.2f" %cost)
    #     return
    # if args.abort:
    #     result = abort_operation(submission['operation'])
    #     #If we can successfully place lapdog-submission-id and lapdog-execution-role labels
    #     #on worker VMs, then we can run:
    #     #gcloud compute instances delte (
    #     # gcloud compute instances list --filter="label:lapdog-execution-role=worker AND label:lapdod-submission-id={submission_id}" \
    #     # | awk 'NR>1{print $1}'
    #     # )
    #     #Alternatively, have cromwell_driver write a list of workflow ids to gs://.../workspace/workflow_ids.json
    #     #["id",...]
    #     #Then abort by getting instances with --filter="label:cromwell-workflow-id=cromwell-{workflow_id}"
    #     if result.returncode and not ('done' in status and status['done']):
    #         print("Failed to abort:", result.stdout.decode())
    #     print("Workflow(s) aborted")
    #     return

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
