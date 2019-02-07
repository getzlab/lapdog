# Changelog

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
