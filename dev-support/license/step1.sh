#!/bin/sh -x

SCRIPT_DIR=$(dirname "${BASH_SOURCE-$0}")
OUTPUT_DIR=$SCRIPT_DIR/
pushd $SCRIPT_DIR/../../
mvn license:aggregate-add-third-party -X -e > $OUTPUT_DIR/mvn.out

cp target/generated-sources/license/THIRD-PARTY.txt $OUTPUT_DIR
popd

echo "Please do the following before continuing:\n \
 Open goodle spreadsheet https://docs.google.com/spreadsheets/d/1jpeVlwydkgM01FNW4GPdAgzch5kuLC8ka09yiewKy7w/edit#gid=1885055871, \
 save these to local:\n \
 'Licenses' tab to licenses.tsv\n \
 'Overrides' tab to overrides.tsv\n \
 'Notices' tab to notices.tsv\n \
 'parse.py script' tab to parse.py\n \
 'standardize.py' tab to standardize.py\n \
 'generate.py script' tab to generate.py"
