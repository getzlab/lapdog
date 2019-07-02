# Changelog

## 0.17.3 (Beta)

Bug Fixes:
* Added support for Dockstore methods
* Fixed sometimes breaking proxy service accounts during workspace creation

Other Changes:
* Cromwell logs should now always be uploaded, even if the submission crashes
* Added a spinner while launching submissions
* Submission errors now include `error-details` key in submission.json

## 0.17.2 (Beta)

Bug Fixes:
* Fixed a bug with certain expressions not evaluating correctly

## 0.17.1 (Beta)

**Deprecation Warning:** `lapdog.gateway.get_access_token()` is now deprecated and will be removed when Lapdog shifts to full release.
If you need to authenticate as the current user, use `lapdog.gateway.get_user_session()`, which returns a prebuilt
`AuthorizedSession` object which can be used to make authenticated requests on behalf of the user. This function only attaches the
OAuth scopes needed by Lapdog, so if you need additional scopes,
use `lapdog.cloud.utils.generate_default_session()`, which allows
requesting arbitrary scopes

**Deprecation Warning:** Changing the _query\_limit_ parameter to `lapdog.WorkspaceManager.execute()`
is no longer supported and will be removed when Lapdog shifts to full release.

New Features:
* Added a workaround for Authorized Domain workspaces

Other Changes:
* Restricted usage scopes of access tokens
* Standardized functions for getting current gcloud accounts
* From now on, newly deployed Lapdog functions will not support unauthenticated access.
This will not change lapdog's api, however individuals calling Lapdog engine functions
outside the client will need to pass an `Authorization` header, or use `lapdog.gateway.get_user_session`,
which returns a `requests.session` object with your authorization token autofilled

## 0.17.0 (Beta)

Bug Fixes:
* Fixed a bug which sometimes prevented functions from being deployed during a patch
* Fixed a bug in the preflight system which sometimes displayed outdated preflight results
* Fixed a missing success notification when uploading new method configurations

New Features:
* Lapdog now supports an optional per-workspace call-cache

Other Changes:
* Updated Dalmatian to 0.0.11
* Updated UI Dependencies

### Additional Cromwell Changes

These additional changes have also been backported to the global Cromwell images used
by Lapdog versions 0.15.0-0.16.4.

* Significantly improved latency of Cromwell log
* Updated to Cromwell 41. This allows worker VMs to be labeled by their parent submission id

### Patch Contents:
* Updated `submit` endpoint to v7 to accommodate call-cache

## 0.16.4 (Beta)

Other Changes:
* Updated `resolve` internal endpoint to v4 (no patch necessary)
    * This change raises requirements for inserting a namespace resolution
    * Redacted previous versions
* Added additional checks at the start of engine initialization to check required
Firecloud permissions prior to making any changes

### Patch Contents

This patch is non-critical and does not contain any endpoint updates.

* Added role definition for `Engine_Admin` role

## 0.16.3 (Beta)

Bug Fixes:
* Fixed an offline workspace with a cold cache disabling ui (#87)
* Fixed page navigation logging an error to the console during workspace sync

Other Changes:
* Updated required version of Google Cloud SDK to `241.0.0`
* During startup, lapdog will print any critical alerts to the terminal
* Lowered the offline threshold for `WorkspaceManager.execute` to 100 entities
* Significant speed improvements to `WorkspaceManager.execute` when preparing a
submission in offline mode
* Aborting a submission now generates a Hound log entry
* UI link previews timeout after 10s and display a fallback dialog
* Speed improvements to `WorkspaceManager.mop`, `WorkspaceManager.list_submissions`, and UI link previews

## 0.16.2 (Beta)

Bug Fixes:
* **Critical:** Fixed an error preventing new function deployments
* Fixed a bug preventing new Engine initialization

Other Changes:
* `lapdog initialize-project` now checks required gcloud version first
* Updated Dalmatian to latest
* Updated `resolve` internal endpoint to v3 (no patch necessary)

### Patch Contents
* Updated `submit` to v6
* Updated `abort` to v2
* Updated `register` to v3
* Updated `signature` to v2
* Updated `query` to v2
* Updated `quotas` to v4

## 0.16.1 (Beta)

New Features:
* Added global alert system to Lapdog UI, allowing for real-time updates to be posted
by Lapdog maintainers

Bug Fixes:
* Fixed bucket previews not being able to copy paths to clipboard
* Fixed pagination allowing you to get to blank pages
* Fixed some CSS scoping issues

## 0.16.0 (Beta)

Breaking Changes:
* `safe_getblob` renamed to `strict_getblob`
* `lapdog.operations.Operator` class removed and integrated with `dalmatian.WorkspaceManager` and `lapdog.WorkspaceManager`
* removed attribute `.operator` from `lapdog.WorkspaceManager`
* `lapdog.WorkspaceManager.operator.get_entities_df` renamed to `lapdog.WorkspaceManager._get_entities_internal`
* `lapdog.Workspacemanager.operator.pending` renamed to `lapdog.WorkspaceManager.pending_operations`
    * Removed previous `lapdog.WorkspaceManager.pending_operations` property
* `lapdog.WorkspaceManager.execute_preflight` renamed to `lapdog.WorkspaceManager.preflight`
* `lapdog.WorkspaceManager.operator.get_config_detail` renamed to `lapdog.WorkspaceManager.get_config`
* `lapdog.WorkspaceManager.prepare_*_df` renamed to `lapdog.WorkspaceManager.upload_entity_metadata`
* Removed module `lapdog.operations`
* Removed `lapdog.provenance`. Use `lapdog.WorkspaceManager.attribute_provenance` and `lapdog.WorkspaceManager.entity_provenance`

New Features:
* Added a previewer for `gs://` links, enabled for most UI elements

Other Changes
* Phased out the Terra UI
* `lapdog.WorkspaceManager.preflight` now returns a namedtuple
* `lapdog.WorkspaceManager` now uses [Hound](https://pypi.org/project/hound/) to log changes to workspace
* `lapdog.provenance` now used [Hound](https://pypi.org/project/hound/) to parse provenance

## 0.15.13 (Beta)

Bug Fixes:
* Fixed a missing file in the UI framework

## 0.15.12 (Beta)

Bug Fixes:
* Fixed a bug preventing lapdog from automatically adding your own proxy group to
new workspaces when you create them
* Fixed a bug causing data to be uploaded and overwritten numerous times while attempting
to translate arrays

## 0.15.11 (Beta)

New Features:
* When arrays are present in entity data, `lapdog.WorkspaceManager`s attempt to silently translate to the appropriate FireCloud format.
* Added Terra upgrade banner to the UI. Use it to opt into a new Terra-style UI

Other Changes:
* Updated UI's Jquery version to 3.4.0
* Improved error handling in all cloud endpoints. This change is not critical so
it will not trigger a patch. Newly created/patched Engines will receive this change.

## 0.15.10 (Beta)

Bug Fixes:
* Fixed error message not being displayed when Firecloud is completely offline
* Fixed UI not being able to start jobs or validate job inputs
* Fixed a crash caused by outdated protobuf

Other Changes:
* Slider CSS now works in multiple browsers

##  0.15.9 (Beta)

New Features:
* Added pagination to UI when listing submissions and workflows

Bug Fixes:
* Fixed a bug which would sometimes display submissions of the wrong workspace in the UI

Other Changes:
* Improved pagination of data table
* Improved error handling of resolution setup
  * Updated `resolve` global endpoint to `v2` (no patch necessary)
* Renamed `lapdog.gateway.generate_core_key` to `__generate_core_key`
  * This change reflects the fact that this function should not be called by end-users

## 0.15.8 (Beta)

New Features:
* Added syntax highlighting when viewing WDLs in the UI
  * The first time you view a configuration in the UI after updating and after clearing your
  lapdog cache, the configuration will be slow to load while lapdog downloads
  WomTool

Bug Fixes:
* Fixed the offline evaluation schema returning unexpected results when attributes
are not defined for one or more entities

Other Changes:
* `lapdog.WorkspaceManager.get_config` now supports multiple argument syntaxes
  * Check the docstring for details, but essentially, if you have anything that
  could be used to identify a method configuration, it will try and take it
* `lapdog.WorkspaceManager.get_adapter` now accepts global submission ids, forwarding
them to the proper `WorkspaceManager` if necessary
* `lapdog.WorkspaceManager.execute` now attempts robust input checking while preparing
submission. Missing required parameters or invalid array parameters will now raise
an exception before requesting to start the submission

## 0.15.7 (Beta)

Bug Fixes:
* Fixed bold text not appearing on black background terminals

## 0.15.6 (Beta)

New Features:
* Added a _target\_set_ argument to `lapdog.WorkspaceManager.update_participant_entities`
  * Allows restricting the participant entities update to a specific entity set

Other Changes:
* Fixed dependency pins

## 0.15.5 (Beta)

Bug Fixes:
* Fixed an issue preventing lapdog from launching due to a dependency version conflict

## 0.15.4 (Beta)

New Features:
* `lapdog.WorkspaceManager`s will attempt to connect with a running lapdog UI (if present)
  to initialize the operator cache with data from the UI's cache of this workspace
* Added autocomplete to config inputs when editing a method configuration in the UI

Bug Fixes:
* Fixed not being able to upload new method configurations if the version was inferred from "latest"
* Fixed being unable to submit jobs or check quotas if a regions file was not defined
* Fixed a bug in the patching process which prevented patches to projects without a regions file

Other Changes:
* `lapdog.WorkspaceManager.build_retry_set` now handles FireCloud submissions
* Synchronized methods of `lapdog.Operator` now have the proper docstrings
* Updated dependencies

### Patch Contents
* Updated `quotas` endpoint to v3
* Updated `submit` endpoint to v5

## 0.15.3 (Beta)

New Features:
* Added autocomplete for the `Entity` field in the UI when running a new job

Bug Fixes:
* `lapdog.gateway.quota_usage` now returns quota usage from all enabled compute regions, not just `us-central1`

Other Changes:
* `lapdog.prune_cache` now prints the size of data removed and kept, and returns the size of data removed

### Patch Contents
* Updated `quotas` endpoint to v2 to support the above bugfix

## 0.15.2 (Beta)

New Features:
* Added `lapdog.copyblob` and `lapdog.moveblob` to copy and move blobs

Bug Fixes:
* Fixed a bug in the CLI preventing new workspaces from being created using `lapdog workspace`
* Fixed a bug in offline expression evaluation which prevented some complex expressions from being parsed

Other Changes:
* Cleaned the error messages displayed in `lapdog doctor`
* `lapdog.Gateway` now has a better string representation

## 0.15.1 (Beta)

New Features:
* Added `lapdog.provenance` to provide provenance of provided data

Bug Fixes:
* `lapdog.Gateway.compute_regions` now silently handles permission errors by returning the default compute region (`us-central1`)
* Fixed a bug which prevented users from re-selecting the default compute region in the UI after selecting another one
* Fixed a bug which allowed some protected files to be deleted by `lapdog.WorkspaceManager.mop()`

## 0.15.0 (Beta)

New Features:
* Users can now choose a custom compute region when submitting jobs
  * Administrators can change the list of allowed regions. [Read more](https://github.com/broadinstitute/lapdog/wiki/Instructions-for-Admins#compute-regions)
  * Added dropdown menu to UI to select the compute region for a job

Other Changes:
* Removed the `zones` argument to `lapdog.WorkspaceManager.execute` in favor of `regions` argument to set compute region for job

### Patch Contents
* Updated `submit` endpoint to v4 to support customizable compute regions
* Updated cromwell tag to `v0.15.0`

## 0.14.2 (Beta)

New Features:
* Added `lapdog doctor` command to diagnose issues with lapdog

Other Changes:
* Resubmitting a job through the UI now retains the same network configuration as the original job
* Interrupting `lapdog.WorkspaceManager.mop()` with Ctrl+C will abort the process and return the results so far

## 0.14.1 (Beta)

Bug Fixes:
* Fixed the cost estimate in the UI displaying the wrong cost for the default VM size

Other Changes:
* `lapdog.WorkspaceManager.gateway` is no longer set to `None` if the namespace has no resolution.
    * The attribute will be a full `lapdog.Gateway` which behaves as expected

## 0.14.0 (Beta)

New Features:

* User can now configure if submissions run with or without an externally routable IP address.
    * Without an IP (default for UI only) the instance cannot reach the internet (except for Google)

Bug Fixes:
* Fixed an issue with new submissions not caching properly

Other Changes:
* Reduced the number of gcloud queries when patching a namespace

### Patch Contents
* Updated `submit` to v3 to reflect the private IP change
* Updated cromwell tag to `v0.14.0`
* Update VPC configuration to meet requirements for running jobs without externally routable IP address

## 0.13.6 (Beta)

New Features:
* Added `lapdog.prune_cache()` to reduce offline cache size by removing entries which have not been accessed in the last 30 days
* Added input configuration data to adapters:
    * `lapdog.adapters.SubmissionAdapter.config` : (Property) A pandas DataFrame representing the submission's input configuration
    * `lapdog.adapters.SubmissionAdapter.input_mapping`: (Property) A dictionary mapping workflow output keys to workflow inputs from the submission configuration
        * This attribute may raise a `FileNotFoundError`. If so, the submission configuration could not be found
    * `lapdog.adapters.WorkflowAdapter.inputs`: (Property) A dictionary of input names to values representing the inputs to this workflow
        * This attribute may raise a `KeyError`. If so, try updating the parent `lapdog.adapters.SubmissionAdapter`
* Lapdog UI now displays workflow inputs when viewing a workflow, if the input mapping was available

Other Changes:
* Updated Python and NodeJS dependencies for Python3.7
* Improved submission caching so that submissions started locally can be immediately cached
* Prevented anonymous access of resolution objects

### Patch Contents
* Added an additional component to resolution patching. This fixes an issue preventing buckets from authenticating in old namespaces

## 0.13.5 (Beta)

Other Changes:
* Improved performance of `lapdog.WorkspaceManager.mop()`
* Added a status bar for `lapdog.WorkspaceManager.update_participant_entities()`

## 0.13.4 (Beta)

New Features:
* Added `lapdog.WorkspaceManager.mop()` method to remove unreferenced data from workspace bucket
* Added `lapdog.WorkspaceManager.acl` and `lapdog.WorkspaceManager.update_acl()` to get and set workspace ACL
* Added `lapdog.adapters.Call.read_log()` to read stderr, stdout, and cromwell logs

Other Changes:
* `lapdog.WorkspaceManager.create_submission` now runs a preflight to better handle input parameters
* "Data" header on the Workspace homepage in the UI now links to FireCloud
* Offline `lapdog.operations.Operator`s will now synchronize to FireCloud every minute,
but remain offline regardless of the result

## 0.13.3 (Beta)

Other Changes:
* Switched Lapdog Namespace Resolution system so as not to take advantage of FireCloud
* Lapdog API CORS policy now only allows the UI

### Patch Contents
* Switched to newer namespace resolution method

## 0.13.2 (Beta)

Bug Fixes:
* Fixed a `NameError` when loading a Gateway that does not exist

## 0.13.1 (Beta)

Other Changes:
* Removed the 1 month lifetime limit on offline cache entries
* Hid Lapdog resolution projects from UI workspace list

## 0.13.0 (Beta)

Bug Fixes:
* Fixed a bug that could allow malicious users to impersonate a Lapdog Engine

### Patch Contents
* Updated `submit` and `register` to v2 to reflect the bug fix above
* Redacted `alpha`, `beta` of all endpoints, as well as `v1` of `submit` and `register`

## 0.12.5 (Beta)

Bug Fixes:
* Fixed a fatal issue in `lapdog initialize-project`
* Pinned `firecloud>=0.16.9`

## 0.12.4 (Beta)

Bug Fixes:
* Fixed an issue preventing `lapdog.operations` from patching `firecloud.api.__SESSION` in some installations

## 0.12.3 (Beta)

Bug Fixes:
* Resolved a bug in `lapdog initialize-project` which prevented billing accounts
from linking properly
* Resolved a bug in `lapdog.Operator` which occasionally caused a `TypeError` when
interacting with the FireCloud API

New Features:
* Added a permissions check to `lapdog initialize-project` to halt the process before
any changes are made if the user does not have sufficient permissions to the provided
billing account

## 0.12.2 (Beta)

Bug Fixes:
* Resolved a bug in the `register` endpoint which prevented the Lapdog proxy group from being created

## 0.12.1 (Beta)

Bug Fixes:
* Resolved a bug in the UI controllers which prevented automatic gateway registration
* Resolved a bug in the cloud utilities which prevented endpoints from querying the list of available cryptographic keys
* Resolved a bug in `setup.py` which prevented the `lapdog.cloud` module from being distributed

### Patch Contents
* Updated role definitions for `Core_account` and `Functions_account`

## 0.12.0 (Beta)

Bug Fixes:
* Fixed a bug preventing Lapdog from recognizing Engines as initialized
* Fixed a bug preventing some Google Cloud timestamps from being parsed

New Features:
* Added the `lapdog apply-patch` function
* The operations cache now uses a timeout when reading from FireCloud
    * 5s timeout for data which is already present in the cache
    * 30s timeout for data which is not cached
* Parallelized `lapdog.WorkspaceManager.update_participant_entities`
* Listing submissions now falls back on the slower `google-cloud-storage` backend
if `gsutil` is not visible on the PATH
* Submission data can be updated by calling `lapdog.SubmissionAdapter.update_data()`
* Added docstrings to all classes and methods which make up the primary lapdog interface
* `lapdog.SubmissionAdapter.cost()` now also reports the portion of the cost which is due to the Cromwell server

### Patch Contents
* All endpoints set to version `v1`. Serial versioning will be used from now on
* Updated the Cromwell image tag to `v0.12.0`

## 0.11.5 (Beta)

Bug Fixes:
* Prevented `lapdog.WorkspaceManager` from re-synchronizing the workspace after executing a job, if the workspace was previously offline
* Fixed an error that was preventing Cromwell from retying queries if a previous query timed out

New Features:
* Lapdog Cloud Endpoints now use the latest cryptograpic key in the project, instead of
hard coding the 1st
* The Cromwell driver now recognizes when Cromwell crashes and adds helpful error messages
* Added a notice on the UI homepage when the local lapdog version is out of date

Other Changes:
* Reorganized `lapdog.cloud` code into a module

## 0.11.4 (Beta)

New Features:
* Added `lapdog.WorkspaceManager.build_retry_set` to automatically build an entity set of failed entities from a submission
* All controllers for the Lapdog UI now log their runtime in the console

Other Changes:
* Improved error messages displayed in UI when submitting a new job fails

## 0.11.3 (Beta)

Other Changes:
* `lapdog.WorkspaceManager.execute` now raises a `ValueError` if it expects that input metadata will exceed the PAPIv2 size limit

## 0.11.2 (Beta)

Bug Fixes:
* `lapdog initialize-project` can now be safely rerun if it fails

## 0.11.1 (Beta)

Bug Fixes:
* `lapdog initialize-project` now enables required billing apis before linking billing accounts
* Removed invalid permissions from IAM Role definitions

## 0.11.0 (Beta)

Bug Fixes:
* Fixed an issue with `lapdog.SubmissionAdapter` not recognizing some workflow events
* Fixed an issue with how some input metadata was unpacked inside Cromwell
* Reduced strain on Cromwell Server API by using a limited connection pool

Other Changes:
* Improved how `lapdog.SubmissionAdapter` estimates cost when the submission is still live

## 0.10.2 (Beta)

Bug Fixes:
* Fixed `lapdog.SubmissionAdapter` not recognizing extended memory machines as valid
* Fixed not being able to start Cromwell servers with large amounts of memory

New Features:
* Added `quotas` endpoint to Lapdog Engine
* Lapdog UI automatically registers user with Lapdog Engine when visiting workspace
* `lapdog.WorkspaceManager` now automatically adds proxy account to workspaces when creating

Other Changes:
* Lapdog UI now reports number of pending offline operations
* Updated IAM Role definitions

## 0.10.1 (Beta)

Bug Fixes:
* Fixed an issue which prevented users besides the owner of an Engine from querying genomics operations
* Reduced the frequency of FireCloud API errors during registration

New Features:
* Enabled automated project initialization

## 0.10.0 (Beta)

### Lapdog Engine

This version overhauled the Lapdog cloud backend. All interactions with cloud resources
(starting, aborting, and tracking jobs) is handled through a "Gateway" object which
connects a local Lapdog client with cloud resources ("Engine"). Each Lapdog Engine
is responsible for a single FireCloud namespace. Currently Engines must be initialized
manually, but in future, the Gateway will be capable of running one-time setup for
Lapdog Engines.

Bug Fixes:
* Fixed runtime configuration not being passed through Lapdog Gateway
* Fixed `lapdog.cloud.update_iam_policy` using wrong session object

New Features:
* Bark! Bark!

Other Changes:
* Improved UI feedback for Lapdog Engine state
* Lapdog Cloud `register` endpoint can now be re-run if it fails partway
* Removed Lapdog Gateway warning from UI
* Lapdog UI Workspace cache is now a switch instead of a button with multiple states
* Updated ui dependencies (Please run `lapdog ui --install` again to update)

---

Versions of Lapdog prior to `0.10.0` did not use a Gateway or Engine.
These versions utilized your personal Google Cloud account and project to
run jobs. These versions are no longer supported, and changes will be recorded very
briefly

### 0.9.9 (Deprecated, Alpha)

Responded to a change in the FireCloud API

### 0.9.8 (Deprecated, Alpha)

Added disk usage to cost estimation

### 0.9.7 (Deprecated, Alpha)

Improved cost estimation

### 0.9.6 (Deprecated, Alpha)

Added Call runtime calculation

### 0.9.5 (Deprecated, Alpha)

Fixed disk cache running out of available filenames

### 0.9.4 (Deprecated, Alpha)

SubmissionAdapters are now thread-safe

### 0.9.3 (Deprecated, Alpha)

Fixed Cost estimation in the UI

### 0.9.2 (Deprecated, Alpha)

Improved submission page UI preformance

### 0.9.1 (Deprecated, Alpha)

Improved cost estimation

### 0.9.0 (Deprecated, Alpha)

Improved Cromwell's ability to handle large submissions

### 0.8.10 and earlier

Changes prior to 0.9.0 are not tracked. These versions, as well as all versions prior to 0.10.0,
are deprecated. If you need to access change history, you can look at commits prior
to [c6b1e12](https://github.com/broadinstitute/lapdog/commit/c6b1e12ffcf679e6ec3f4e88debb02a2007a2899)
