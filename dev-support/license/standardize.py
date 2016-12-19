#!/usr/bin/env python

import os
import pdb
import re
import sys
import traceback

# No need to list ASLv2 dependencies in LICENSE.
# See http://www.apache.org/dev/licensing-howto.html#alv2-dep
OMIT_IN_LICENSES = {"ASLv2"}

# See https://issues.apache.org/jira/browse/HADOOP-12893?focusedCommentId=15284739&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15284739
OMIT_IN_NOTICES = {"2-clause BSD", "3-clause BSD", "4-clause BSD", "MIT"}

# Anything not in here needs manual inspection. Populated when reading in Licenses
ACCEPTED_LICENSES = set()

#https://www.apache.org/legal/resolved#category-x
CATEGORY_X_LICENSES = {"BCL", "NPL", "GPL", "LGPL", "CPOL"}

OVERRIDE_LICENSES = {}

license_name_resolution = {}

def read_licenses(licenses_file):
    lines = open(licenses_file, "r").readlines()
    lineNo=0
    try:
        for line in lines:
            lineNo += 1
            cols = line.split("\t")
            name = cols[0]
            if name == "License":
                # skip the first line
                continue
            ACCEPTED_LICENSES.add(name)
            text = cols[1]
            full_names = cols[3].split("|")
            for full_name in full_names:
                full_name = full_name.rstrip()
                if not full_name:
                    continue;
                license_name_resolution[full_name] = name
    except:
        print "error reading licenses at line", lineNo
        traceback.print_exc()

    # print "license_name_resolution", license_name_resolution
    # print "ACCEPTED_LICENSES", ACCEPTED_LICENSES

def read_overrides(override_file):
    lines = open(override_file, "r").readlines()
    lineNo=0
    try:
        for line in lines:
            lineNo += 1
            cols = line.split("\t")
            name = cols[0].rstrip()
            if name == "Name":
                # skip the title line
                continue
            lic = cols[3].rstrip()
            if lic:
                OVERRIDE_LICENSES[name] = lic
    except:
        print "error reading overrides at line", lineNo
        traceback.print_exc()

    # print "OVERRIDE_LICENSES", OVERRIDE_LICENSES

notices = {}
def read_notices(notices_file):
    lines = open(notices_file, "r").readlines()
    lineNo=0
    try:
        for line in lines:
            lineNo += 1
            cols = line.split("\t")
            name = cols[0].rstrip()
            if name == "Dependency" or not cols[1]:
                # skip the title line and dependencies without License
                print "skipping invalid line #%d:" % lineNo, line
                continue
            notice = cols[2].rstrip()
            notices[name] = notice
    except:
        print "error reading notices at line", lineNo
        traceback.print_exc()

    # print "notices", notices

read_licenses("licenses.tsv")
read_overrides("overrides.tsv")
read_notices("notices.tsv")

# go through all dependencies and check license.
manual_licenses = {}
manual_notices = set()
lines = open("parsed.tsv", "r").readlines()
output = open("standardized.tsv", "w")
lineNo=0
try:
    for line in lines:
        lineNo += 1
        cols = line.rstrip().split("\t")
        if len(cols) < 3:
            print "skipping line", lineNo, ":", line
            continue
        name = cols[0].rstrip()
        if name in OVERRIDE_LICENSES:
            license_name = OVERRIDE_LICENSES[name]
        else:
            license_name = cols[1]
        if license_name in license_name_resolution:
            license_name = license_name_resolution[license_name]
        if license_name not in ACCEPTED_LICENSES:
            # try splitting and check again
            tokens = license_name.split(",")
            for token in tokens:
                if token in license_name_resolution:
                    token = license_name_resolution[token]
                if token in ACCEPTED_LICENSES:
                    license_name = token
                    break
            else:
                #print "license name '%s' not found in supplied licenses" % license_name;
                manual_licenses[name] = license_name
                # if we don't know the license, notices must also be inspected.
                manual_notices.add(name)
        notice = ""
        if name not in notices:
            if license_name not in OMIT_IN_NOTICES:
                # only non-omitted notices need inspection
                manual_notices.add(name)
        else:
            notice = notices[name]
        output.write(name + "\t" + license_name + "\t" + notice +  "\t" + cols[2] + "\n")
except:
    print "error reading parsed at line", lineNo
    traceback.print_exc()
output.close()

print "*" * 20 + "The following licenses require human inspection" + "*" * 20
for n, l in manual_licenses.iteritems():
    print "Dependency %s, license: %s" % (n, l)
print "*" * 90


print "*" * 20 + "The following notices require human inspection" + "*" * 20
for n in manual_notices:
    print "%s" % (n)
print "*" * 90


# lines = open("parsed.tsv", "r").readlines()
# for line in lines:
#     lineNo += 1
#     cols = line.rstrip().split("\t")
#     if len(cols) < 3:
#         print "skipping line", lineNo, ":", line
#         continue
#     name = cols[0].rstrip()
#     if name in manual_notices:
#         print line