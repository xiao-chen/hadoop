#!/bin/sh -x

SCRIPT_DIR=$(dirname "${BASH_SOURCE-$0}")
pushd $SCRIPT_DIR
python parse.py > parsed.tsv
popd
