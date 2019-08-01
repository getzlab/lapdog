FROM ubuntu:16.04
MAINTAINER Aaron Graubert

# Dockerfile for lapdog self-updater

RUN apt-get update && apt-get install -y \
  software-properties-common \
  build-essential \
  python3 \
  python3-pip \
  git \
  curl

# Update pip
RUN python3 -m pip install --upgrade pip setuptools

# Install gcloud
# See https://cloud.google.com/sdk/
RUN curl https://sdk.cloud.google.com | bash

# Add the install location explicitly to the path (for non-interactive shells)
ENV PATH /root/google-cloud-sdk/bin:$PATH

COPY update.sh /

RUN chmod 755 /update.sh
