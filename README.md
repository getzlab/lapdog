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

### TODO
1. Rework submission adapter reader
  - On first call: create singleton reader object
  - On all calls:
    - Poll singleton for data and store to buffer
    - Return BytesIO of buffer
2. Improve caching latency
3. Sort out adapter cache hierarchy
4. Enable call caching
