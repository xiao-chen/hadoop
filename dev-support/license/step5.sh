#!/bin/sh -x

SCRIPT_DIR=$(dirname "${BASH_SOURCE-$0}")
pushd $SCRIPT_DIR/../../
for f in $(grep -r -i copyright . | awk -F ":" '{print $1}' |uniq ) ;do grep -L "http://www.apache.org/licenses/LICENSE-2.0" "$f";done > copyright.txt 
popd
