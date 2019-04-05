import argparse
import pandas as pd
import lapdog
from .api.__main__ import run as ui_main
from . import __version__
import os
import tempfile
import json
import subprocess
import sys
import io
import crayons

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
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='lapdog '+__version__,
        help="Display the current version and exit"
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
        'config',
        type=argparse.FileType('r'),
        help="Configuration to upload. If methodRepoMethod.methodVersion"
        " is set to 'latest', the version will be set to the latest method snapshot"
        " including a new upload with the --wdl argument",
        default=None
    )
    method_parser.add_argument(
        '-w', '--wdl',
        type=argparse.FileType('r'),
        help="WDL to upload",
        default=None
    )
    method_parser.add_argument(
        '-n', '--method-name',
        help="The name of the uploaded method. This argument is ignored if the"
        " --wdl argument is not provided. By default, this equals the methodRepoMethod.methodName key.",
        default=None
    )
    method_parser.add_argument(
        '-a', '--namespace',
        help="The namespace to upload the method and configuration."
        " By default, this equals"
        " the namespace from the methodRepoMethod.methodNamespace key.",
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

    ui_parser = subparsers.add_parser(
        'ui',
        help="Starts the web API for lapdog",
        description="Starts the web API for lapdog"
    )
    ui_parser.set_defaults(func=lambda args: ui_main(args))
    ui_parser.add_argument(
        '-a', '--api-only',
        action='store_false',
        help="Do not launch the ui",
        dest='vue'
    )
    ui_parser.add_argument(
        '--install',
        action='store_true',
        help="Installs the node dependencies to run the UI"
    )
    ui_parser.add_argument(
        '-p', '--port',
        type=int,
        help="Set the port for the browser UI. (Default: 4200)",
        default=4200,
        dest='ui_port'
    )
    ui_parser.add_argument(
        '--api-port',
        type=int,
        help="Set the port for the backend API. (Default: 4201)",
        default=4201,
        dest='port'
    )

    service_account_parser = subparsers.add_parser(
        'initialize-project',
        help="One-time initialization for the lapdog execution backend",
        description="One-time setup for the lapdog execution backend"
    )
    service_account_parser.set_defaults(func=cmd_service_account)

    patch_parser = subparsers.add_parser(
        'apply-patch',
        help="Apply pre-packaged upgrades to the Lapdog Engine for a given namespace",
        description="Apply pre-packaged upgrades to the Lapdog Engine for a given namespace"
    )
    patch_parser.set_defaults(func=cmd_patch)
    patch_parser.add_argument(
        'namespace',
        help="The firecloud namespace to patch"
    )

    doc_parser = subparsers.add_parser(
        'doctor',
        help='Diagnose issues with lapdog',
        description='Diagnose issues with lapdog'
    )
    doc_parser.set_defaults(func=cmd_doc)
    doc_parser.add_argument(
        'namespaces',
        nargs='*',
        help="Diagnose issues with the Lapdog Engine for given Firecloud Namespace(s)",
        default=[]
    )
    # service_account_parser.add_argument(
    #     'email',
    #     help="Your firecloud account email"
    # )

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
        ws.create_workspace(parent=args.create)

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
    if 'sample_id' not in df.columns or 'participant_id' not in df.columns:
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

    args.config = json.load(args.config)
    args.workspace.update_configuration(args.config, args.wdl, args.method_name, args.namespace)

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
    return yaml.safe_load(
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

def cmd_finish(args):
    print("Note: lapdog-finish is not yet fully implemented")
    lapdog.complete_execution(args.submission_id)

def cmd_service_account(args):
    print("Lapdog Engine Initialization")
    print(
        crayons.yellow("WARNING:", bold=True),
        "This runs one-time setup for an entire Firecloud",
        crayons.normal("Namespace", bold=True)
    )
    print("This should be considered an analagous action to creating a Firecloud Billing Project")
    print("You must be an owner of the Firecloud Billing Project and a Billing Account User for the underlying Google Billing Account")
    print("Press Enter to continue, or Ctrl+C to abort")
    try:
        input()
    except KeyboardInterrupt:
        print()
        print("Aborted. No action has been taken")
        return
    print(
        crayons.yellow("WARNING:", bold=True),
        "Please read all prompts carefully as incorrect information can cause a corrupt Lapdog Engine for the Namespace"
    )
    print()
    namespace = input("Enter the Firecloud Namespace: ")
    namespace_c = input("Confirm the Firecloud Namespace: ")
    billing = input("Enter the Google Billing Account ID: ")
    acct = input("Enter your Firecloud/GCP Email: ")
    if namespace != namespace_c:
        print(
            crayons.red("Error:", bold=True),
            "Namespace does not match"
        )
        sys.exit("%s != %s"  %(namespace, namespace_c))
    from lapdog.gateway import get_account, get_access_token, get_token_info
    acct_c = get_account()
    if acct != acct_c:
        print(
            crayons.red("Error:", bold=True),
            "The provided account (%s) does not match the currently logged in account (%s)" % (
                acct,
                acct_c
            )
        )
        sys.exit("Please run `gcloud auth login` followed by `gcloud config set account %s`" % acct)
    info = get_token_info(get_access_token())
    if info['email'] != acct:
        print(
            crayons.red("Error:", bold=True),
            "The provided account (%s) does not match the current application-default credentials (%s)" % (
                acct,
                info['email']
            )
        )
        sys.exit("Please run `gcloud auth application-default login`")
    from lapdog.gateway import ld_project_for_namespace
    print("==========================")
    print("Ready to Initialize")
    print("1) Create Google Project", crayons.normal(ld_project_for_namespace(namespace), bold=True))
    print("    - This project will contain Lapdog Services and Resources to execute jobs in the", crayons.normal(namespace, bold=True), "namespace")
    print("2) Link Project", crayons.normal(ld_project_for_namespace(namespace), bold=True), "to Google Billing Project", crayons.normal(billing, bold=True))
    print("    - All charges for Lapdog will be billed to this account")
    print("    - Charges include costs for running jobs, storing Lapdog metadata, and operating the Lapdog Engine")
    print("    - Data storage costs will be billed through the associated Firecloud Workspaces")
    print("3) Enable Execution Engine")
    print("    - Store initial metadata in the project")
    print("    - Upload and activate Google Cloud Functions to operate the Lapdog Engine")
    print("    - Create Lapdog Service Accounts and IAM Roles")
    print("4) Grant Admin Access")
    print("    - Your account will have administrator access to the project")
    print("    - You must grant access to any other users which need administrator access to the project")
    print("    - You (or any other administrators) will be responsible for maintaining the project")
    print("        *", crayons.blue("https://github.com/broadinstitute/lapdog/wiki/Instructions-for-Admins", bold=True))
    print("    - End-Users will automatically be granted User access by demonstrating WRITER permissions to any workspace in the namespace")
    print()
    print("Press Enter to Start, or Ctrl+C to abort")
    try:
        input()
    except KeyboardInterrupt:
        print()
        print("Aborted. No action has been taken")
        return
    from lapdog.gateway import Gateway
    Gateway.initialize_lapdog_for_project(billing, namespace)
    print("==========================")
    print("Initialization complete")
    print()
    print("Please read", crayons.normal("https://github.com/broadinstitute/lapdog/wiki/Instructions-for-Admins", bold=True))


def cmd_patch(args):
    from .cloud.patch import __project_admin_apply_patch
    __project_admin_apply_patch(args.namespace)

def cmd_doc(args):
    from pip._internal.utils.misc import get_installed_distributions
    from pkg_resources import get_distribution
    from semver import match as ver_match
    import traceback
    import re

    pattern = re.compile(r'((\d+\.){0,2}\d+.*?)')

    def recursive_dependent_check(pkg, parent=None):
        try:
            dist = get_distribution(pkg.project_name)
            for spec in pkg.specs:
                try:
                    if pattern.match(dist.version):
                        ver = pattern.match(dist.version).group(1)
                        while len(ver.split('.')) < 3:
                            ver += '.0'
                    if not ver_match(ver, ''.join(spec)):
                        return False, (parent + '->' + dist.project_name if parent is not None else dist.project_name), dist.version +''.join(spec)
                except ValueError:
                    pass
            for subpackage in dist.requires():
                result = recursive_dependent_check(subpackage, (parent + '->' + dist.project_name if parent is not None else dist.project_name))
                if result[0] is False:
                    return result
            return True, dist.version
        except:
            return (None,)

    print("Diagnosing issues with local lapdog installation")
    print('----------------------')
    print(crayons.normal("Lapdog Version:", bold=True), __version__)
    print("Lapdog dependencies")
    ld = get_distribution('lapdog')
    for req in ld.requires():
        result = recursive_dependent_check(req)
        if result[0] is None:
            print(req.project_name, crayons.cyan("Unable to validate dependency"))
        elif result[0] is False:
            print(req.project_name, crayons.red("Unmet dependency:"))
            print("   ", result[1], result[2])
        else:
            print(req.project_name, crayons.green(result[1]))
    if len(args.namespaces):
        print('======================')
        print("Diagnosing Namespaces")
        from .cloud.utils import __API_VERSION__
        for namespace in args.namespaces:
            print('----------------------')
            print(crayons.normal(namespace, bold=True), end=' ')
            with lapdog.capture() as (out, err):
                gateway = lapdog.Gateway(namespace)
                out.seek(0,0)
                out.truncate()
                err.seek(0,0)
                err.truncate()
            if not gateway.exists:
                print(crayons.red("Engine not found"))
                print("The Engine for this namespace may not be initialized")
                print("Or it may not have been patched since version 0.13.2")
                continue
            fail = False
            for endpoint, version in __API_VERSION__.items():
                if endpoint != 'resolve':
                    with lapdog.capture() as (out, err):
                        try:
                            gateway.get_endpoint(endpoint)
                        except:
                            out.seek(0,0)
                            out.truncate()
                            err.seek(0,0)
                            err.truncate()
                            fail = True
                            break
            if not fail:
                print(crayons.green("Engine OK"))
            else:
                # Bad style, but oh well. This allows us to reference the final values of endpoint and version
                # and print to stdout directly. Inside capture, stdout is no longer a tty
                print(crayons.red("Endpoint {} (version {}) not found".format(endpoint, version)))
                print("The Engine for this namespace needs to be patched")


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
