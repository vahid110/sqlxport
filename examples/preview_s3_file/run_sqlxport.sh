#!/bin/bash
set -e

sqlxport run \
  --s3-bucket my-bucket \
  --s3-key users.parquet \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --preview-s3-file \
  --s3-endpoint https://s3.amazonaws.com
