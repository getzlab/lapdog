# Security Policy

## Patches

Lapdog handles updates through its patching system. Currently, patches must be applied manually.
It is imperative that administrators keep up-to-date with Lapdog and patch their namespaces regularly.

## Reporting a Vulnerability

If you think you have found a vulnerability which effects the security or integrity of Lapdog, please carefully follow these steps:

1. **DO NOT** open an issue on GitHub or otherwise publicly post about the vulnerability
    * Reporting any security vulnerability should follow the principles of
    [Responsible Disclosure](https://en.wikipedia.org/wiki/Responsible_disclosure).
2. Please send an email to `aarong@broadinstitute.org` and CC `francois@broadinstitute.org` and `birger@broadinstitute.org`
    * Describe the problem in as much detail as possible
    * Someone will get back to you ASAP

Here are some example criteria of security vulnerabilities.
If you think your bug can be described by any of the following, treat it as a security vulnerability:

* Allows users to access data they otherwise could not access, particularly data belonging to other users
* Allows users to directly access or modify cloud components that they do not have
permissions to access or modify directly. Particularly:
    * Ability to modify the source code or IAM policies of cloud functions
    * Ability to modify the IAM policies of the project
    * Ability to access the core signing account's access key
    * Ability to utilize any compute resource besides resources provisioned through normal job execution
    * Ability to modify the configuration of any compute networks or subnetworks
    * Ability to create, list, or cancel and genomics operations (all users have `GET` access to operations)
    * Ability to authenticate as any service account in the project
* Allows users to run arbitrary code outside the context of the docker container for a workflow
* Allows users to modify or falsify a Lapdog Resolution (which ties a Firecloud Namespace to a particular project)
