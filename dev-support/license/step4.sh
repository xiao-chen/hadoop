#!/bin/sh -x

SCRIPT_DIR=$(dirname "${BASH_SOURCE-$0}")
pushd $SCRIPT_DIR
python generate.py
popd
