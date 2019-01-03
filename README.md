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
    - Install `node` and `npm` if you don't already have them installed
    - Run `lapdog ui --install`. This may take a while
3. (Optional) Enable the Lapdog Execution Service
    - Run `lapdog init {your account email}`
        * `{your account email}` should be the email registered to your account in firecloud
        * This requires that you have "Services Admin", "Compute Admin", and "Genomics Admin" on your current Google Cloud Project
    - If this succeeds, the Lapdog Execution Service will be enabled on your current Google Cloud Project
    - Execution must be enabled on each Firecloud workspace by granting WRITER access to your service account

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

---

### Roadmap
(Subject to change)
1. ~~Add Data view~~ **Done**
2. Enable Multi-Project mode
3. Add better timeouts when interacting with Firecloud
    * Lapdog should switch to its internal cache more eagerly than it does now
    * Goal: enforce a ~5s timeout if the desired data is already cached
    * 20s timeout otherwise
4. Enable call caching

### Pro/Con with Firecloud

##### Pros
* Each submission has a dedicated Cromwell instance. Your jobs will never queue, unless you hit a Google usage quota
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
* **Lapdog uses your personal GCloud account**. Lapdog does not operate using the billing account of the workspace
  * In the future, Lapdog will use a dedicated service account for each Firecloud billing account, but this is still in development and is a 'far-future' feature
* You pay an additional 5¢/hour fee for each submission to run the Cromwell server
* There is no call cache in Lapdog yet. This is still in development
* Submission results must be manually uploaded to Firecloud by clicking the `Upload Results` button in the UI.
