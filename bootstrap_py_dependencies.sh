#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
sudo pip install -U \
  pandas \
  pyspark \
  sklearn \
  scipy \
  s3fs
