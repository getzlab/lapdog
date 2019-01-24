# Lapdog

[![PyPI](https://img.shields.io/pypi/v/lapdog.svg)](https://pypi.io/project/lapdog)

A relaxed wrapper for dalmatian and FISS

## Requirements
Lapdog requires the Google Cloud SDK, which can be installed [here](https://cloud.google.com/sdk/)

## Installing
1. Install lapdog via pip: `pip install lapdog`
    - If you already have lapdog installed, you can upgrade it with
    `pip install --upgrade lapdog`
2. (Optional) Enable the Lapdog User Interface:
    - The UI runs locally by default. If you are installing Lapdog on a server, you'll
    need to set up an SSH tunnel for ports 4200 and 4201
    - Install `node` and `npm` if you don't already have them installed
        - If you're on Mac OS, run `brew install node npm`
    - Run `lapdog ui --install`. This may take a while

## Usage
1. `lapdog` may be imported within python as a drop-in replacement for `dalmatian`
    - lapdog presents a superset of features available in dalmatian
    - `WorkspaceManager`s in lapdog cache data when communicating with Firecloud.
    If Firecloud experiences an intermittent failure, the `WorkspaceManager` may be
    able to continue running in offline mode. Calling `WorkspaceManager.sync()` will
    reconnect to Firecloud, pushing out any data updates that were queued while in offline mode
    - `WorkspaceManager`s in lapdog present the execution api via `WorkspaceManager.execute()`.
    Executions differ from submissions in that they run directly on Google and results are
    uploaded back to Firecloud afterwards
2. `lapdog` may be used as a command line tool.
    - The tool provides the necessary functions to create a workspace, fill it with data,
    import or upload methods and configurations, and submit jobs (or execute them directly)
    - Run `lapdog --help` to get the list of available commands
3. `lapdog` may be used via an interactive user interface which serves to run and
  monitor lapdog executions
    - Run `lapdog ui` to launch the user interface

## Job Execution

Lapdog executes jobs through dedicated Google Projects ("Engines") for each FireCloud Namespace.
A Lapdog Engine can only be initialized for a given Namespace by a billing account admin.
To initialize a new Engine, contact your Namespace admin and ask them to run `lapdog initialize-project`.

After an Engine is initialized, you will have to register with it:

* The Lapdog User Interface will automatically register you when you load a workspace
in a namespace that you're not registered to
* The Lapdog python module supports manual registration
    * When you create a `WorkspaceManager` in an unregistered Namespace, you will get a warning
    * You can also check your registration status by checking the value of `WorkspaceManager.gateway.registered`
    * You can then register by using `WorkspaceManager.gateway.register()`
    * If registration fails due to any FireCloud errors, simply wait a few minutes
    then try calling `register()` again
* The Lapdog CLI does not support registration. You can register through the UI or
python module

### Workspace Permissions

In the UI, at the bottom of every page, you will find a **firecloud.org** email.
This is a proxy group email which contains you, and all your service accounts.
To allow the Lapdog Engine to run jobs, that proxy group email must be granted
WRITE access to FireCloud workspaces where jobs will run. You may grant the group
READ access to workspaces where data will be read from, but jobs cannot execute
in workspaces without WRITE permissions. The proxy group email can be found by
calling `lapdog.cloud.proxy_group_for_user(YOUR_EMAIL)`.

**NOTE:** Due to a bug in FireCloud, permissions will not be granted if the group
was already granted access to a workspace before you registered to that namespace's
Lapdog Engine. If your proxy email definitely was granted access to a workspace,
but your jobs are still failing with permissions errors, try removing access and then
re-granting it. You can see FireCloud's response to this bug report [here](https://gatkforums.broadinstitute.org/firecloud/discussion/23350/account-not-inheriting-permissions-when-added-to-group)

---

### Roadmap
(Subject to change)
1. ~~Add Data view~~ **Done**
2. ~~Enable Multi-Project mode~~ **Done**
3. Add better timeouts when interacting with Firecloud
    * Lapdog should switch to its internal cache more eagerly than it does now
    * Goal: enforce a ~5s timeout if the desired data is already cached
    * 20s timeout otherwise
4. Enable call caching

### Pro/Con with Firecloud

##### Pros
* Each submission has a dedicated Cromwell instance. Your jobs will never queue, unless you hit a Google usage quota
* Lapdog supports Requester Pays buckets and GPUs
* Workspace cache: Lapdog caches most data received from Firecloud.
    * In the event of a Firecloud error, Lapdog will attempt to keep running by using it's cached data. Any data updates will by pushed back to Firecloud when the workspace is synced
* Data caches: The Lapdog API caches data sent to the UI and read from Google
    * These caches greatly improve UI performance by storing results whenever possible
* Streamlined UI: The Lapdog UI was built with efficiency in mind
* Quality of life features:
    * Save time updating methods. Set `methodRepoMethod.methodVersion` to "latest" and let Lapdog figure out what the snapshot ID is
    * Easy data uploads. Call `prepare_entity_df` on a DataFrame before uploading to Firecloud. Any local filepaths will be uploaded to the workspace's bucket in the background and a new DataFrame will be returned containing the new `gs://` paths
    * Automatic reference uploads. When you call `update_attributes`, any values which refer to local filepaths will be uploaded in the background (just like `prepare_entity_df`). `update_attributes` now returns a dictionary containing the attributes exactly as uploaded

##### Cons
* You pay an additional 5¢/hour fee for each submission to run the Cromwell server
* There is no call cache in Lapdog yet. This is still in development
* Submission results must be manually uploaded to Firecloud by clicking the `Upload Results` button in the UI.
* There are small overhead costs billed to the Lapdog Engine for operation. These costs
are for calls to the API and for storage of metadata, both of which should be very cheap
