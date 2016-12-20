#!/bin/sh -x

SCRIPT_DIR=$(dirname "${BASH_SOURCE-$0}")
pushd $SCRIPT_DIR/../../

STAGING_FILE=files_with_copyright.txt
grep -r -i -E "copyright|©" . | awk -F ":" '{print $1}' |uniq > $STAGING_FILE
OLDIFS=$IFS
IFS=$'\n'
for f in $(cat $STAGING_FILE) ;do grep -L "http://www.apache.org/licenses/LICENSE-2.0" "$f";done > copyright.txt
IFS=$OLDIFS
popd
