# Copyright 2017 Google Inc.
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

FROM java:openjdk-8-jre
MAINTAINER Aaron Graubert

# Fix apt repositories
RUN rm /etc/apt/sources.list.d/jessie-backports.list && \
  printf "deb http://archive.debian.org/debian/ jessie main\ndeb http://security.debian.org jessie/updates main" > /etc/apt/sources.list


#Install mysql
RUN apt-get update && \
    echo 'mysql-server mysql-server/root_password password cromwell' | debconf-set-selections && \
    echo 'mysql-server mysql-server/root_password_again password cromwell' | debconf-set-selections && \
    apt-get install mysql-server -y

# Install python
RUN apt-get update && \
    apt-get install python --yes && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install gcloud
# See https://cloud.google.com/sdk/
RUN curl https://sdk.cloud.google.com | bash

# Add the install location explicitly to the path (for non-interactive shells)
ENV PATH /root/google-cloud-sdk/bin:$PATH

# Install pip for the next two steps...
RUN apt-get update && \
    apt-get install python-pip --yes

# Install Python "requests" (for cromwell_driver.py) package
RUN pip install requests simplejson cryptography==0.6.1

# Install Google Python client (for file_util.py) package
RUN pip install --upgrade google-api-python-client oauth2client

# Remove pip
RUN apt-get remove --yes python-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy Cromwell and the Cromwell conf template
RUN mkdir /cromwell
RUN cd /cromwell && \
    curl -L -O https://github.com/broadinstitute/cromwell/releases/download/41/cromwell-41.jar
RUN ln /cromwell/cromwell-41.jar /cromwell/cromwell.jar
COPY jes_template.tmp.conf /cromwell/

# Copy the wdl_runner python, shell script, and dependencies
RUN mkdir /wdl_runner
COPY cromwell_driver.py \
     file_util.py \
     sys_util.py \
     wdl_outputs_util.py \
     wdl_runner.py \
     wdl_runner.sh \
     logger.py \
     /wdl_runner/
RUN chmod u+x /wdl_runner/wdl_runner.sh

# Set up the runtime environment
ENV CROMWELL /cromwell/cromwell.jar
ENV CROMWELL_CONF /cromwell/jes_template.conf

WORKDIR /wdl_runner
