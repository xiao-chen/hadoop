#!/usr/bin/env python

import os
import sys
import re
import traceback

licenses = {}
notices = {}
notice_order = []   # ordering the dep names under the same notice

def write_license(file, license, license_book, *deps):
    content=""
    for deplist in deps:
        for dep in deplist:
            content += dep + "\n"

    if len(content) > 0:
        file.write("For:\n")
        file.write(content)
        file.write("-" * 80 + "\n")
        file.write("(" + license + ")\n")
        if license in license_book:
            file.write(license_book[license] + "\n\n")
        else:
            file.write(license + "\n\n")

def write_notice(file, notice, *deps):
    content=""
    for deplist in deps:
        for dep in deplist:
            content += dep + ",\n"

    if len(content) > 0:
        file.write("The binary distribution of this product bundles binaries of\n")
        file.write(content)
        file.write("which has the following notices:\n")
        file.write(" * " + notice + "\n\n")

lines = open("standardized.tsv", "r").readlines()
lineNo=0
try:
    for line in lines:
        lineNo += 1
        cols = line.split("\t")
        if len(cols) < 3:
            print "skipping line ", lineNo
            continue
        if cols[0] == "":
            print "skipping empty line ", lineNo
            continue
        name = cols[0]
        license = cols[1]
        notice = cols[2]
        # notice_name = cols[4].replace(" - no url defined", "")
        version = re.search(':([0-9.]*[0-9.]+)', cols[3])
        if version is not None:
            name += version.group(0).replace(":", " ")
        # bundled = cols[7]
        # if bundled not in {"Y", "N", "only"}:
        #     print "skipping unknown bundle type at line ", lineNo
        #     continue

        if license != "":
            if license == "ASLv2":
                # No need to list ASLv2 dependencies in LICENSE.
                # See http://www.apache.org/dev/licensing-howto.html#alv2-dep
                print "Not including '%s' in LICENSE since it's ASLv2" %(name)
            else:
                if license not in licenses:
                    # licenses[license] = {"Y": [], "N": [], "only": []}
                    licenses[license] = []
                licenses[license].append(name)
        if notice != "":
            if license in ("2-clause BSD", "3-clause BSD", "4-clause BSD", "MIT"):
                # See https://issues.apache.org/jira/browse/HADOOP-12893?focusedCommentId=15284739&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-15284739
                print "Skipping NOTICE of '%s' since its license is %s" %(name, license)
                continue
            if notice not in notices:
                # notices[notice] = {"Y": [], "N": [], "only": []}
                notices[notice] = []
                notice_order.append(notice)
            notices[notice].append(name)
except:
    print "error at line", lineNo
    traceback.print_exc()

license_book = {}
lines = open("licenses.tsv", "r").readlines()
lineNo=0
try:
    for line in lines:
        lineNo += 1
        cols = line.split("\t")
        text = cols[1]
        if len(text) > 0:
            license_book[cols[0]]=text
except:
    print "error at line", lineNo

src = open("licenses", "w")
for l, deps in licenses.iteritems():
    if l == "Public Domain":
        print "The following dependencies are Public Domain, hence skipped"
        print deps
        continue
    write_license(src, l, license_book, deps)
src.close()

src = open("notices", "w")
for ordered_notice in notice_order:
    write_notice(src, ordered_notice, notices[ordered_notice])
src.close()

